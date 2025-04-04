package s3

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/tempo/pkg/util"
)

const (
	SignatureVersionV4 = "v4"
	SignatureVersionV2 = "v2"

	// SSEKMS config type constant to configure S3 server side encryption using KMS
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
	SSEKMS = "SSE-KMS"

	// SSES3 config type constant to configure S3 server side encryption with AES-256
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
	SSES3 = "SSE-S3"
)

var (
	supportedSSETypes = []string{SSEKMS, SSES3}

	errUnsupportedSSEType = errors.New("unsupported S3 SSE type")
)

type SSEConfig struct {
	Type                 string `yaml:"type"`
	KMSKeyID             string `yaml:"kms_key_id"`
	KMSEncryptionContext string `yaml:"kms_encryption_context"`
}

type Config struct {
	tls.ClientConfig `yaml:",inline"`

	Bucket            string         `yaml:"bucket"`
	Prefix            string         `yaml:"prefix"`
	Endpoint          string         `yaml:"endpoint"`
	Region            string         `yaml:"region"`
	AccessKey         string         `yaml:"access_key"`
	SecretKey         flagext.Secret `yaml:"secret_key"`
	SessionToken      flagext.Secret `yaml:"session_token"`
	Insecure          bool           `yaml:"insecure"`
	PartSize          uint64         `yaml:"part_size"`
	HedgeRequestsAt   time.Duration  `yaml:"hedge_requests_at"`
	HedgeRequestsUpTo int            `yaml:"hedge_requests_up_to"`
	// SignatureV2 configures the object storage to use V2 signing instead of V4
	SignatureV2      bool              `yaml:"signature_v2"`
	ForcePathStyle   bool              `yaml:"forcepathstyle"`
	UseDualStack     bool              `yaml:"enable_dual_stack"`
	BucketLookupType int               `yaml:"bucket_lookup_type"`
	Tags             map[string]string `yaml:"tags"`
	StorageClass     string            `yaml:"storage_class"`
	Metadata         map[string]string `yaml:"metadata"`
	// Deprecated
	// See https://github.com/grafana/tempo/pull/3006 for more details
	NativeAWSAuthEnabled  bool      `yaml:"native_aws_auth_enabled"`
	ListBlocksConcurrency int       `yaml:"list_blocks_concurrency"`
	SSE                   SSEConfig `yaml:"sse"`
}

func (cfg *Config) RegisterFlagsAndApplyDefaults(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Bucket, util.PrefixConfig(prefix, "s3.bucket"), "", "s3 bucket to store blocks in.")
	f.StringVar(&cfg.Prefix, util.PrefixConfig(prefix, "s3.prefix"), "", "s3 root directory to store blocks in.")
	f.StringVar(&cfg.Endpoint, util.PrefixConfig(prefix, "s3.endpoint"), "", "s3 endpoint to push blocks to.")
	f.StringVar(&cfg.AccessKey, util.PrefixConfig(prefix, "s3.access_key"), "", "s3 access key.")
	f.StringVar(&cfg.MinVersion, util.PrefixConfig(prefix, "s3.tls_min_version"), "VersionTLS12", "minimum version of TLS to use when connecting to s3.")
	f.Var(&cfg.SecretKey, util.PrefixConfig(prefix, "s3.secret_key"), "s3 secret key.")
	f.Var(&cfg.SessionToken, util.PrefixConfig(prefix, "s3.session_token"), "s3 session token.")
	f.IntVar(&cfg.ListBlocksConcurrency, util.PrefixConfig(prefix, "s3.list_blocks_concurrency"), 3, "number of concurrent list calls to make to backend")

	f.StringVar(&cfg.SSE.Type, util.PrefixConfig(prefix, "s3.sse.type"), "", fmt.Sprintf("Enable AWS Server Side Encryption. Supported values: %s.", strings.Join(supportedSSETypes, ", ")))
	f.StringVar(&cfg.SSE.KMSKeyID, util.PrefixConfig(prefix, "s3.sse.kms-key-id"), "", "KMS Key ID used to encrypt objects in S3")
	f.StringVar(&cfg.SSE.KMSEncryptionContext, util.PrefixConfig(prefix, "s3.sse.kms-encryption-context"), "", "KMS Encryption Context used for object encryption. It expects JSON formatted string.")
	cfg.HedgeRequestsUpTo = 2
}

func (cfg *Config) PathMatches(other *Config) bool {
	// S3 bucket names are globally unique
	return cfg.Bucket == other.Bucket && cfg.Prefix == other.Prefix
}
