package drain

import (
	"strings"
	"time"

	"github.com/prometheus/common/model"
)

type LogCluster struct {
	id         int
	Size       int
	Tokens     []string
	TokenState interface{}
	Stringer   func([]string, interface{}) string
	cache      string
}

func (c *LogCluster) String() string {
	if c.cache != "" {
		return c.cache
	}
	if c.Stringer != nil {
		c.cache = c.Stringer(c.Tokens, c.TokenState)
		return c.cache
	}
	c.cache = strings.Join(c.Tokens, "")
	return c.cache
}

func (c *LogCluster) GetTokens() []string {
	return c.Tokens
}

func (c *LogCluster) append(ts model.Time, maxChunkAge time.Duration, sampleInterval time.Duration) {
	c.Size++
}

func (c *LogCluster) merge() {
	// c.Size += int(sumSize(samples))
}

func (c *LogCluster) Iterator(lvl string, from, through, step, sampleInterval model.Time) {
}

func (c *LogCluster) Samples() {
}

func (c *LogCluster) Prune(olderThan time.Duration) {
}
