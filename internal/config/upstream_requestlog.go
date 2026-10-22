package config

import "fmt"

const (
	// DefaultUpstreamRequestLogMaxSizeMB is the size at which the request log
	// file is rotated when upstream.requestLog.maxSizeMB is not set.
	DefaultUpstreamRequestLogMaxSizeMB = 10
	// DefaultUpstreamRequestLogMaxBackups is how many rotated files are kept
	// when upstream.requestLog.maxBackups is not set.
	DefaultUpstreamRequestLogMaxBackups = 3
)

// UpstreamRequestLogConfig turns on a persistent JSON-lines audit log of the
// /upstream/<model>/* requests made to the listed models.
type UpstreamRequestLogConfig struct {
	// Path is the log file. Empty disables the log.
	Path string `yaml:"path,omitempty"`
	// Models are the model IDs (or aliases) whose upstream requests are logged.
	Models []string `yaml:"models,omitempty"`
	// MaxSizeMB rotates the file once it would grow past this many MiB.
	MaxSizeMB int `yaml:"maxSizeMB,omitempty"`
	// MaxBackups is the number of rotated files (path.1 ... path.N) kept.
	MaxBackups int `yaml:"maxBackups,omitempty"`
}

// Enabled reports whether the log is configured.
func (c UpstreamRequestLogConfig) Enabled() bool { return c.Path != "" }

// normalize applies defaults and validates the section.
func (c *UpstreamRequestLogConfig) normalize() error {
	if c.Path == "" {
		if len(c.Models) > 0 {
			return fmt.Errorf("upstream.requestLog: models is set but path is empty")
		}
		return nil
	}
	if len(c.Models) == 0 {
		return fmt.Errorf("upstream.requestLog: models must list at least one model when path is set")
	}
	if c.MaxSizeMB < 0 || c.MaxBackups < 0 {
		return fmt.Errorf("upstream.requestLog: maxSizeMB and maxBackups must not be negative")
	}
	if c.MaxSizeMB == 0 {
		c.MaxSizeMB = DefaultUpstreamRequestLogMaxSizeMB
	}
	if c.MaxBackups == 0 {
		c.MaxBackups = DefaultUpstreamRequestLogMaxBackups
	}
	return nil
}
