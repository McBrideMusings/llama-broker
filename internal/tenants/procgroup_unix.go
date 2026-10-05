//go:build !windows

package tenants

import (
	"os/exec"
	"syscall"
)

// killTreeOnCancel starts cmd in its own process group and, when its context
// ends, kills the whole group. A child that inherited the output pipe dies with
// the command, so the pipe closes at the deadline instead of WaitDelay later.
func killTreeOnCancel(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
}
