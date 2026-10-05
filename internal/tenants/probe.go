package tenants

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
)

// maxProbeBody caps how much of a probe or action response is read.
const maxProbeBody = 1 << 20

// probeResult is one probe reading. raw describes what was observed (status
// code, JSON value, exit code) so every log line carries the real values.
type probeResult struct {
	ok  bool
	raw string
	err error
}

func (r probeResult) String() string {
	if r.err != nil {
		return fmt.Sprintf("%v (error: %v)", r.ok, r.err)
	}
	return fmt.Sprintf("%v (%s)", r.ok, r.raw)
}

// runProbe evaluates p. A probe that cannot be evaluated reads false with err
// set; neither the condition poll nor the drain takes it as a reading.
func runProbe(ctx context.Context, p *config.TenantProbe) probeResult {
	if p.Cmd != "" {
		code, out, err := runCmd(ctx, p.Cmd)
		raw := fmt.Sprintf("exit %d", code)
		if out != "" {
			raw += ": " + out
		}
		return probeResult{ok: err == nil && code == 0, raw: raw, err: err}
	}

	status, body, err := doHTTP(ctx, p.Method, p.URL, "")
	if err != nil {
		return probeResult{err: err}
	}
	raw := fmt.Sprintf("HTTP %d", status)
	if status != p.Status {
		return probeResult{raw: raw + fmt.Sprintf(", want %d", p.Status)}
	}
	if p.JSON == "" {
		return probeResult{ok: true, raw: raw}
	}
	value, err := lookupJSON(body, p.JSON)
	if err != nil {
		return probeResult{raw: raw, err: err}
	}
	encoded, _ := json.Marshal(value)
	return probeResult{ok: truthy(value), raw: fmt.Sprintf("%s, %s=%s", raw, p.JSON, encoded)}
}

// runAction performs a and describes the outcome for the log.
func runAction(ctx context.Context, a *config.TenantAction) (string, error) {
	if a.Cmd != "" {
		code, out, err := runCmd(ctx, a.Cmd)
		if err == nil && code != 0 {
			err = fmt.Errorf("exit %d", code)
		}
		return strings.TrimSpace(fmt.Sprintf("exit %d %s", code, out)), err
	}
	status, body, err := doHTTP(ctx, a.Method, a.URL, a.Body)
	if err != nil {
		return "", err
	}
	raw := strings.TrimSpace(fmt.Sprintf("HTTP %d %s", status, strings.TrimSpace(string(body))))
	if status < 200 || status > 299 {
		return raw, fmt.Errorf("HTTP %d", status)
	}
	return raw, nil
}

func doHTTP(ctx context.Context, method, url, body string) (int, []byte, error) {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxProbeBody))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("reading body: %w", err)
	}
	return resp.StatusCode, data, nil
}

// runCmd runs cmdStr and returns its exit code and trimmed combined output. err
// is set only when the command could not run or was cut off by ctx.
func runCmd(ctx context.Context, cmdStr string) (int, string, error) {
	args, err := config.SanitizeCommand(cmdStr)
	if err != nil {
		return -1, "", err
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	killTreeOnCancel(cmd)
	// Backstop for a child that left the process group and still holds the
	// output pipe: CombinedOutput gives up waiting for it after this.
	cmd.WaitDelay = time.Second
	out, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if len(text) > 200 {
		text = text[:200] + "..."
	}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, text, nil
	case ctx.Err() != nil:
		return -1, text, ctx.Err()
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), text, nil
	default:
		return -1, text, err
	}
}

// lookupJSON walks a dot path ("a.b.0.c", leading dot optional) through a JSON
// document. Numeric segments index arrays. A missing key yields nil.
func lookupJSON(body []byte, path string) (any, error) {
	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("body is not JSON: %w", err)
	}
	cur := doc
	for _, seg := range strings.Split(strings.TrimPrefix(path, "."), ".") {
		switch node := cur.(type) {
		case map[string]any:
			cur = node[seg]
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(node) {
				return nil, nil
			}
			cur = node[i]
		default:
			return nil, nil
		}
	}
	return cur, nil
}

// truthy is true for true, a non-zero number, and a non-empty string, array or
// object.
func truthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case float64:
		return x != 0
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	default:
		return false
	}
}
