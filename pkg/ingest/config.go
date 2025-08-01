package ingest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/flagext"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// writerRequestTimeoutOverhead is the overhead applied by the Writer to every Kafka timeout.
	// You can think about this overhead as an extra time for requests sitting in the client's buffer
	// before being sent on the wire and the actual time it takes to send it over the network and
	// start being processed by Kafka.
	writerRequestTimeoutOverhead = 2 * time.Second

	// producerBatchMaxBytes is the max allowed size of a batch of Kafka records.
	producerBatchMaxBytes = 16_000_000

	// maxProducerRecordDataBytesLimit is the max allowed size of a single record data. Given we have a limit
	// on the max batch size (producerBatchMaxBytes), a Kafka record data can't be bigger than the batch size
	// minus some overhead required to serialise the batch and the record itself. We use 16KB as such overhead
	// in the worst case scenario, which is expected to be way above the actual one.
	maxProducerRecordDataBytesLimit = producerBatchMaxBytes - 16384
	minProducerRecordDataBytesLimit = 1024 * 1024
)

var (
	ErrMissingKafkaAddress               = errors.New("the Kafka address has not been configured")
	ErrMissingKafkaTopic                 = errors.New("the Kafka topic has not been configured")
	ErrInconsistentConsumerLagAtStartup  = errors.New("the target and max consumer lag at startup must be either both set to 0 or to a value greater than 0")
	ErrInvalidMaxConsumerLagAtStartup    = errors.New("the configured max consumer lag at startup must greater or equal than the configured target consumer lag")
	ErrInvalidProducerMaxRecordSizeBytes = fmt.Errorf("the configured producer max record size bytes must be a value between %d and %d", minProducerRecordDataBytesLimit, maxProducerRecordDataBytesLimit)
	ErrInconsistentSASLCredentials       = errors.New("the SASL username and password must be both configured to enable SASL authentication")
)

type Config struct {
	Enabled bool        `yaml:"enabled"`
	Kafka   KafkaConfig `yaml:"kafka"`
}

func (cfg *Config) RegisterFlagsAndApplyDefaults(prefix string, f *flag.FlagSet) {
	cfg.Kafka.RegisterFlagsWithPrefix(prefix, f)
}

func (cfg *Config) Validate() error {
	if !cfg.Enabled {
		return nil
	}

	return cfg.Kafka.Validate()
}

// KafkaConfig holds the generic config for the Kafka backend.
type KafkaConfig struct {
	Address      string        `yaml:"address"`
	Topic        string        `yaml:"topic"`
	ClientID     string        `yaml:"client_id"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`

	SASLUsername string         `yaml:"sasl_username"`
	SASLPassword flagext.Secret `yaml:"sasl_password"`

	ConsumerGroup                     string        `yaml:"consumer_group"`
	ConsumerGroupOffsetCommitInterval time.Duration `yaml:"consumer_group_offset_commit_interval"`

	LastProducedOffsetRetryTimeout time.Duration `yaml:"last_produced_offset_retry_timeout"`

	AutoCreateTopicEnabled           bool `yaml:"auto_create_topic_enabled"`
	AutoCreateTopicDefaultPartitions int  `yaml:"auto_create_topic_default_partitions"`

	ProducerMaxRecordSizeBytes int   `yaml:"producer_max_record_size_bytes"`
	ProducerMaxBufferedBytes   int64 `yaml:"producer_max_buffered_bytes"`

	TargetConsumerLagAtStartup time.Duration `yaml:"target_consumer_lag_at_startup"`
	MaxConsumerLagAtStartup    time.Duration `yaml:"max_consumer_lag_at_startup"`

	ConsumerGroupLagMetricUpdateInterval time.Duration `yaml:"consumer_group_lag_metric_update_interval"`

	// The fetch backoff config to use in the concurrent fetchers (when enabled). This setting
	// is just used to change the default backoff in tests.
	concurrentFetchersFetchBackoffConfig backoff.Config `yaml:"-"`
}

func (cfg *KafkaConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("kafka", f)
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+".address", "localhost:9092", "The Kafka backend address.")
	f.StringVar(&cfg.Topic, prefix+".topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+".client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+".dial-timeout", 2*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
	f.DurationVar(&cfg.WriteTimeout, prefix+".write-timeout", 10*time.Second, "How long to wait for an incoming write request to be successfully committed to the Kafka backend.")

	f.StringVar(&cfg.SASLUsername, prefix+".sasl-username", "", "The SASL username for authentication.")
	f.Var(&cfg.SASLPassword, prefix+".sasl-password", "The SASL password for authentication.")

	f.StringVar(&cfg.ConsumerGroup, prefix+".consumer-group", "", "The consumer group used by the consumer to track the last consumed offset. The consumer group must be different for each ingester. If the configured consumer group contains the '<partition>' placeholder, it is replaced with the actual partition ID owned by the ingester. When empty (recommended), Tempo uses the ingester instance ID to guarantee uniqueness.")
	f.DurationVar(&cfg.ConsumerGroupOffsetCommitInterval, prefix+".consumer-group-offset-commit-interval", time.Second, "How frequently a consumer should commit the consumed offset to Kafka. The last committed offset is used at startup to continue the consumption from where it was left.")

	f.DurationVar(&cfg.LastProducedOffsetRetryTimeout, prefix+".last-produced-offset-retry-timeout", 10*time.Second, "How long to retry a failed request to get the last produced offset.")

	f.BoolVar(&cfg.AutoCreateTopicEnabled, prefix+".auto-create-topic-enabled", true, "Enable auto-creation of Kafka topic if it doesn't exist.")
	f.IntVar(&cfg.AutoCreateTopicDefaultPartitions, prefix+".auto-create-topic-default-partitions", 1000, "When auto-creation of Kafka topic is enabled and this value is positive, Kafka's num.partitions configuration option is set on Kafka brokers with this value when Tempo component that uses Kafka starts. This configuration option specifies the default number of partitions that the Kafka broker uses for auto-created topics. Note that this is a Kafka-cluster wide setting, and applies to any auto-created topic. If the setting of num.partitions fails, Tempo proceeds anyways, but auto-created topics could have an incorrect number of partitions.")

	f.IntVar(&cfg.ProducerMaxRecordSizeBytes, prefix+".producer-max-record-size-bytes", maxProducerRecordDataBytesLimit, "The maximum size of a Kafka record data that should be generated by the producer. An incoming write request larger than this size is split into multiple Kafka records. We strongly recommend to not change this setting unless for testing purposes.")
	f.Int64Var(&cfg.ProducerMaxBufferedBytes, prefix+".producer-max-buffered-bytes", 1024*1024*1024, "The maximum size of (uncompressed) buffered and unacknowledged produced records sent to Kafka. The produce request fails once this limit is reached. This limit is per Kafka client. 0 to disable the limit.")

	consumerLagUsage := fmt.Sprintf("Set both -%s and -%s to 0 to disable waiting for maximum consumer lag being honored at startup.", prefix+".target-consumer-lag-at-startup", prefix+".max-consumer-lag-at-startup")
	f.DurationVar(&cfg.TargetConsumerLagAtStartup, prefix+".target-consumer-lag-at-startup", 2*time.Second, "The best-effort maximum lag a consumer tries to achieve at startup. "+consumerLagUsage)
	f.DurationVar(&cfg.MaxConsumerLagAtStartup, prefix+".max-consumer-lag-at-startup", 15*time.Second, "The guaranteed maximum lag before a consumer is considered to have caught up reading from a partition at startup, becomes ACTIVE in the hash ring and passes the readiness check. "+consumerLagUsage)

	f.DurationVar(&cfg.ConsumerGroupLagMetricUpdateInterval, prefix+".consumer_group_lag_metric_update_interval", 1*time.Minute, "How often the lag metric is updated. Set to 0 to disable metric calculation and export ")
}

func (cfg *KafkaConfig) Validate() error {
	if cfg.Address == "" {
		return ErrMissingKafkaAddress
	}
	if cfg.Topic == "" {
		return ErrMissingKafkaTopic
	}
	if cfg.ProducerMaxRecordSizeBytes < minProducerRecordDataBytesLimit || cfg.ProducerMaxRecordSizeBytes > maxProducerRecordDataBytesLimit {
		return ErrInvalidProducerMaxRecordSizeBytes
	}
	if (cfg.TargetConsumerLagAtStartup == 0 && cfg.MaxConsumerLagAtStartup != 0) || (cfg.TargetConsumerLagAtStartup != 0 && cfg.MaxConsumerLagAtStartup == 0) {
		return ErrInconsistentConsumerLagAtStartup
	}
	if cfg.MaxConsumerLagAtStartup < cfg.TargetConsumerLagAtStartup {
		return ErrInvalidMaxConsumerLagAtStartup
	}

	if (cfg.SASLUsername == "") != (cfg.SASLPassword.String() == "") {
		return ErrInconsistentSASLCredentials
	}

	return nil
}

// GetConsumerGroup returns the consumer group to use for the given instanceID and partitionID.
func (cfg *KafkaConfig) GetConsumerGroup(instanceID string, partitionID int32) string {
	if cfg.ConsumerGroup == "" {
		return instanceID
	}

	return strings.ReplaceAll(cfg.ConsumerGroup, "<partition>", strconv.Itoa(int(partitionID)))
}

// SetDefaultNumberOfPartitionsForAutocreatedTopics tries to set num.partitions config option on brokers.
// This is best-effort, if setting the option fails, error is logged, but not returned.
func (cfg KafkaConfig) SetDefaultNumberOfPartitionsForAutocreatedTopics(logger log.Logger) {
	if cfg.AutoCreateTopicDefaultPartitions <= 0 {
		return
	}

	cl, err := kgo.NewClient(commonKafkaClientOptions(cfg, nil, logger)...)
	if err != nil {
		level.Error(logger).Log("msg", "failed to create kafka client", "err", err)
		return
	}

	adm := kadm.NewClient(cl)
	defer adm.Close()

	defaultNumberOfPartitions := fmt.Sprintf("%d", cfg.AutoCreateTopicDefaultPartitions)
	_, err = adm.AlterBrokerConfigsState(context.Background(), []kadm.AlterConfig{
		{
			Op:    kadm.SetConfig,
			Name:  "num.partitions",
			Value: &defaultNumberOfPartitions,
		},
	})
	if err != nil {
		level.Error(logger).Log("msg", "failed to alter default number of partitions", "err", err)
		return
	}

	level.Info(logger).Log("msg", "configured Kafka-wide default number of partitions for auto-created topics (num.partitions)", "value", cfg.AutoCreateTopicDefaultPartitions)
}
