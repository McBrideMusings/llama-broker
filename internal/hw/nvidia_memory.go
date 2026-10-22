package hw

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// NvidiaMemoryUsedMiB reads, with a fresh nvidia-smi call, the GPU memory in
// use now, summed over every NVIDIA GPU. It returns errNvidiaSMINotAvailable
// when nvidia-smi is not on PATH.
func NvidiaMemoryUsedMiB(ctx context.Context) (int64, error) {
	if _, err := exec.LookPath("nvidia-smi"); err != nil {
		return 0, errNvidiaSMINotAvailable
	}
	out, err := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits").Output()
	if err != nil {
		return 0, fmt.Errorf("querying nvidia-smi: %w", err)
	}
	return parseNvidiaMemoryUsed(string(out))
}

// IsNvidiaSMIMissing reports whether err means nvidia-smi is not installed.
func IsNvidiaSMIMissing(err error) bool { return err == errNvidiaSMINotAvailable }

// parseNvidiaMemoryUsed sums the one-number-per-GPU lines of nvidia-smi's
// memory.used query.
func parseNvidiaMemoryUsed(out string) (int64, error) {
	var total int64
	var seen int
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		mib, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing nvidia-smi memory.used %q: %w", line, err)
		}
		total += mib
		seen++
	}
	if seen == 0 {
		return 0, fmt.Errorf("nvidia-smi reported no GPU")
	}
	return total, nil
}
