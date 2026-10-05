package tenants

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/event"
	"github.com/mostlygeek/llama-swap/internal/hw"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// reserveTenants is two equal-priority tenants without conditions: big needs
// 6000 MiB, small 1000 MiB.
func reserveTenants(onBlocked string) map[string]config.TenantConfig {
	return map[string]config.TenantConfig{
		"big":   {Members: []string{"big-model"}, Priority: 1, VRAM: 6000, Interval: 1, OnBlocked: onBlocked},
		"small": {Members: []string{"small-model"}, Priority: 1, VRAM: 1000, Interval: 1, OnBlocked: onBlocked},
	}
}

func TestTenants_ReserveHoldsLoadThatCutsIntoIt(t *testing.T) {
	log := logmon.NewWriter(io.Discard)
	m := New(reserveTenants(config.TenantOnBlockedHold), log)
	r := &fakeRouter{running: map[string]process.ProcessState{}}
	m.Start(t.Context(), r)
	m.SetVRAM(8000, 2000)

	// 6000 + 2000 reserve = 8000 fits exactly.
	if reason, _ := m.Block("big-model", nil); reason != nil {
		t.Fatalf("big blocked on an empty card: %v", reason)
	}
	// big running: 6000 + 1000 + 2000 = 9000 > 8000.
	reason, refuse := m.Block("small-model", []string{"big-model"})
	var re *ReserveError
	if !errors.As(reason, &re) || refuse {
		t.Fatalf("small with big running: reason %v refuse %v, want a held ReserveError", reason, refuse)
	}
	if re.StatusCode() != http.StatusServiceUnavailable {
		t.Errorf("status %d want 503", re.StatusCode())
	}
	wantReason := "model small-model (tenant small) is blocked: its vram 1000 MiB plus 6000 MiB of running tenants plus vramReserve 2000 MiB is 9000 MiB, over the 8000 MiB total"
	if reason.Error() != wantReason {
		t.Errorf("reason %q\nwant   %q", reason, wantReason)
	}
	// Already running: its need is already counted.
	if reason, _ := m.Block("big-model", []string{"big-model"}); reason != nil {
		t.Errorf("running big blocked: %v", reason)
	}
	// big is what small's swap evicts, so it is not alongside.
	if reason, _ := m.Block("small-model", nil); reason != nil {
		t.Errorf("small blocked once big is evicted: %v", reason)
	}

	m.Record("small-model", reason, refuse)
	want := `tenant=small model=small-model action=hold probe="vram total=8000MiB reserve=2000MiB running=[big:6000] need=small:1000" reason="` + wantReason + `"`
	if history := string(log.GetHistory()); !strings.Contains(history, want) {
		t.Errorf("log lacks %q\nlog:\n%s", want, history)
	}

	// A stop frees VRAM, so it wakes the scheduler.
	_, before := r.snapshot()
	event.Emit(swaputil.ProcessStateChangeEvent{ProcessName: "big-model", OldState: "stopping", NewState: "stopped"})
	eventually(t, "a wake after a stop", func() bool { _, wakes := r.snapshot(); return wakes > before })

	st := m.Status(map[string]process.ProcessState{"big-model": process.StateReady}, nil)
	if st.VRAM.TotalMiB == nil || *st.VRAM.TotalMiB != 8000 || st.VRAM.ReserveMiB != 2000 || !st.VRAM.Enforced ||
		st.VRAM.UsedMiB != 6000 || len(st.VRAM.Running) != 1 || st.VRAM.Running[0] != (TenantNeed{Tenant: "big", VRAMMiB: 6000}) {
		t.Errorf("vram status %+v, want total 8000, reserve 2000, enforced, big:6000 running", st.VRAM)
	}
	if st.Tenants[0].VRAMMiB != 6000 || st.Tenants[1].VRAMMiB != 1000 {
		t.Errorf("tenant needs %d, %d; want 6000, 1000", st.Tenants[0].VRAMMiB, st.Tenants[1].VRAMMiB)
	}
}

func TestTenants_ReserveWarnsAboutTenantThatCanNeverLoad(t *testing.T) {
	log := logmon.NewWriter(io.Discard)
	m := New(reserveTenants(config.TenantOnBlockedHold), log)
	m.SetVRAM(7000, 2000)
	history := string(log.GetHistory())
	if want := "tenants: big can never load: vram 6000 MiB plus vramReserve 2000 MiB is 1000 MiB over the 7000 MiB total"; !strings.Contains(history, want) {
		t.Errorf("log lacks %q\nlog:\n%s", want, history)
	}
	if strings.Contains(history, "small can never load") {
		t.Errorf("small (1000 MiB) fits but was warned about:\n%s", history)
	}
}

func TestTenants_ReserveRefusesForOnBlockedRefuse(t *testing.T) {
	m := New(reserveTenants(config.TenantOnBlockedRefuse), logmon.NewWriter(io.Discard))
	m.SetVRAM(8000, 2000)
	if reason, refuse := m.Block("small-model", []string{"big-model"}); reason == nil || !refuse {
		t.Fatalf("reason %v refuse %v, want a refused ReserveError", reason, refuse)
	}
}

func TestTenants_ReserveNotEnforcedWithoutTotal(t *testing.T) {
	m := New(reserveTenants(config.TenantOnBlockedHold), logmon.NewWriter(io.Discard))
	m.SetVRAM(0, 2000)
	if reason, _ := m.Block("small-model", []string{"big-model"}); reason != nil {
		t.Fatalf("blocked with no known total: %v", reason)
	}
	st := m.Status(nil, nil)
	if st.VRAM.TotalMiB != nil || st.VRAM.Enforced {
		t.Errorf("vram status %+v, want null total and not enforced", st.VRAM)
	}
}

func TestTenants_TotalVRAMSumsAccelerators(t *testing.T) {
	gib := func(n uint64) *uint64 { b := n << 30; return &b }
	h := &hw.HardwareSnapshot{Accelerators: []hw.Accelerator{
		{Memory: hw.AcceleratorMemory{CapacityBytes: gib(24)}},
		{Memory: hw.AcceleratorMemory{}},
		{Memory: hw.AcceleratorMemory{CapacityBytes: gib(8)}},
	}}
	if got := TotalVRAMMiB(h); got != 32*1024 {
		t.Errorf("total %d MiB want %d", got, 32*1024)
	}
	if got := TotalVRAMMiB(nil); got != 0 {
		t.Errorf("nil snapshot total %d want 0", got)
	}
}
