//go:build windows

package tenants

import (
	"os/exec"
	"strconv"
	"syscall"
)

// killTreeOnCancel kills cmd and every process it started when its context
// ends. A child that inherited the output pipe dies with the command, so the
// pipe closes at the deadline instead of WaitDelay later.
func killTreeOnCancel(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		kill := exec.Command("taskkill", "/f", "/t", "/pid", strconv.Itoa(cmd.Process.Pid))
		kill.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x08000000} // CREATE_NO_WINDOW
		if err := kill.Run(); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
}
