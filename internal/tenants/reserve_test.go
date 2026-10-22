package tenants

import (
	"context"
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
	if reason, _ := m.Block(t.Context(), "big-model", nil); reason != nil {
		t.Fatalf("big blocked on an empty card: %v", reason)
	}
	// big running: 6000 + 1000 + 2000 = 9000 > 8000.
	reason, refuse := m.Block(t.Context(), "small-model", []string{"big-model"})
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
	if reason, _ := m.Block(t.Context(), "big-model", []string{"big-model"}); reason != nil {
		t.Errorf("running big blocked: %v", reason)
	}
	// big is what small's swap evicts, so it is not alongside.
	if reason, _ := m.Block(t.Context(), "small-model", nil); reason != nil {
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
	if reason, refuse := m.Block(t.Context(), "small-model", []string{"big-model"}); reason == nil || !refuse {
		t.Fatalf("reason %v refuse %v, want a refused ReserveError", reason, refuse)
	}
}

func TestTenants_ReserveNotEnforcedWithoutTotal(t *testing.T) {
	m := New(reserveTenants(config.TenantOnBlockedHold), logmon.NewWriter(io.Discard))
	m.SetVRAM(0, 2000)
	if reason, _ := m.Block(t.Context(), "small-model", []string{"big-model"}); reason != nil {
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

// fakeReader is a GPU memory reading the test sets.
type fakeReader struct{ used int64 }

func (f *fakeReader) read(context.Context) (int64, error) { return f.used, nil }

func TestTenants_ReserveCountsExternalUsers(t *testing.T) {
	m := New(reserveTenants(config.TenantOnBlockedHold), logmon.NewWriter(io.Discard))
	m.ConfigureVRAM(8000, VRAMConfig{ReserveMiB: 1000, External: map[string]int{"qmd": 1500, "clip": 500}})

	// 1000 + 2000 external + 1000 reserve = 4000 fits.
	if reason, _ := m.Block(t.Context(), "small-model", nil); reason != nil {
		t.Fatalf("small blocked: %v", reason)
	}
	// 6000 + 2000 external + 1000 reserve = 9000 > 8000, though nothing runs.
	reason, refuse := m.Block(t.Context(), "big-model", nil)
	var re *ReserveError
	if !errors.As(reason, &re) || refuse {
		t.Fatalf("big with external users: reason %v refuse %v, want a held ReserveError", reason, refuse)
	}
	want := "model big-model (tenant big) is blocked: its vram 6000 MiB plus 0 MiB of running tenants plus 2000 MiB of vramExternal plus vramReserve 1000 MiB is 9000 MiB, over the 8000 MiB total"
	if reason.Error() != want {
		t.Errorf("reason %q\nwant   %q", reason, want)
	}
	st := m.Status(nil, nil)
	if st.VRAM.ExternalMiB != 2000 || !st.VRAM.Enforced {
		t.Errorf("vram status %+v, want externalMiB 2000 and enforced", st.VRAM)
	}
}

func TestTenants_ReserveCountsModelVRAMOutsideTenants(t *testing.T) {
	m := New(nil, logmon.NewWriter(io.Discard))
	m.ConfigureVRAM(8000, VRAMConfig{ReserveMiB: 1000, Models: map[string]int{"embed": 600, "llm": 5000, "big": 7000}})

	// 600 + 5000 + 1000 = 6600 fits.
	if reason, _ := m.Block(t.Context(), "llm", []string{"embed"}); reason != nil {
		t.Fatalf("llm blocked beside embed: %v", reason)
	}
	// Already loaded: never blocked.
	if reason, _ := m.Block(t.Context(), "llm", []string{"llm"}); reason != nil {
		t.Fatalf("running llm blocked: %v", reason)
	}
	// 600 + 7000 + 1000 = 8600 > 8000. No tenant, so refused with the 503.
	reason, refuse := m.Block(t.Context(), "big", []string{"embed"})
	var re *ReserveError
	if !errors.As(reason, &re) || !refuse || re.Tenant != "" {
		t.Fatalf("big beside embed: reason %v refuse %v, want a refused ReserveError", reason, refuse)
	}
	if want := "model big is blocked: its vram 7000 MiB plus 600 MiB of running tenants plus vramReserve 1000 MiB is 8600 MiB, over the 8000 MiB total"; reason.Error() != want {
		t.Errorf("reason %q\nwant   %q", reason, want)
	}
	// The embedder's vram counts in the status while it runs.
	st := m.Status(map[string]process.ProcessState{"embed": process.StateReady}, nil)
	if st.VRAM.UsedMiB != 600 || len(st.VRAM.Running) != 1 || st.VRAM.Running[0] != (TenantNeed{Model: "embed", VRAMMiB: 600}) {
		t.Errorf("vram status %+v, want embed:600 running", st.VRAM)
	}
}

func TestTenants_ReserveHoldsLoadWhenMeasuredMemoryIsOverDeclared(t *testing.T) {
	m := New(reserveTenants(config.TenantOnBlockedHold), logmon.NewWriter(io.Discard))
	m.memMaxAge = 0
	gpu := &fakeReader{used: 1000}
	m.ConfigureVRAM(8000, VRAMConfig{ReserveMiB: 500, Reader: gpu.read})

	// Declared 1000 + 500 fits and 1000 in use + 1000 + 500 fits.
	if reason, _ := m.Block(t.Context(), "small-model", nil); reason != nil {
		t.Fatalf("small blocked on a card with 1000 MiB in use: %v", reason)
	}
	// An unknown program takes 6800 MiB: declared still fits, measured does not.
	gpu.used = 6800
	reason, refuse := m.Block(t.Context(), "small-model", nil)
	var re *ReserveError
	if !errors.As(reason, &re) || refuse {
		t.Fatalf("small with 6800 MiB in use: reason %v refuse %v, want a held ReserveError", reason, refuse)
	}
	want := "model small-model (tenant small) is blocked: its vram 1000 MiB plus 6800 MiB of GPU memory in use plus vramReserve 500 MiB is 8300 MiB, over the 8000 MiB total"
	if reason.Error() != want {
		t.Errorf("reason %q\nwant   %q", reason, want)
	}
	if wantProbe := "vram total=8000MiB reserve=500MiB running=[] need=small:1000 measured=6800MiB"; re.Probe() != wantProbe {
		t.Errorf("probe %q want %q", re.Probe(), wantProbe)
	}
	// The memory is freed: the load fits again.
	gpu.used = 1000
	if reason, _ := m.Block(t.Context(), "small-model", nil); reason != nil {
		t.Errorf("small blocked after the memory was freed: %v", reason)
	}
}

func TestTenants_ReserveSubtractsWhatTheSwapEvicts(t *testing.T) {
	m := New(reserveTenants(config.TenantOnBlockedHold), logmon.NewWriter(io.Discard))
	m.memMaxAge = 0
	m.Start(t.Context(), &fakeRouter{running: map[string]process.ProcessState{"big-model": process.StateReady}})
	gpu := &fakeReader{used: 6000}
	m.ConfigureVRAM(8000, VRAMConfig{ReserveMiB: 500, Reader: gpu.read})

	// big-model's 6000 MiB are still in use but the swap evicts it.
	if reason, _ := m.Block(t.Context(), "small-model", nil); reason != nil {
		t.Fatalf("small blocked by the model its swap evicts: %v", reason)
	}
}

func TestTenants_ReportsUnknownVRAMUser(t *testing.T) {
	log := logmon.NewWriter(io.Discard)
	m := New(reserveTenants(config.TenantOnBlockedHold), log)
	m.memMaxAge = 0
	gpu := &fakeReader{used: 8500}
	m.ConfigureVRAM(24000, VRAMConfig{ReserveMiB: 500, External: map[string]int{"qmd": 2000}, Reader: gpu.read})
	running := map[string]process.ProcessState{"big-model": process.StateReady}

	// 8500 in use less 6000 running less 2000 external.
	st := m.Status(running, nil)
	if st.VRAM.MeasuredMiB == nil || *st.VRAM.MeasuredMiB != 8500 || st.VRAM.UnknownMiB == nil || *st.VRAM.UnknownMiB != 500 {
		t.Fatalf("vram status measured %v unknown %v, want 8500 and 500", st.VRAM.MeasuredMiB, st.VRAM.UnknownMiB)
	}
	if st.VRAM.ExternalMiB != 2000 {
		t.Errorf("externalMiB %d want 2000", st.VRAM.ExternalMiB)
	}
	line := "tenants: unknown VRAM user: 500 MiB"
	if got := strings.Count(string(log.GetHistory()), line); got != 1 {
		t.Fatalf("warning logged %d times, want 1\nlog:\n%s", got, log.GetHistory())
	}
	// A steady figure is logged once.
	m.Status(running, nil)
	if got := strings.Count(string(log.GetHistory()), line); got != 1 {
		t.Errorf("steady unknown logged %d times, want 1", got)
	}
	// Under the 256 MiB threshold nothing is warned about.
	gpu.used = 8200
	m.Status(running, nil)
	if got := strings.Count(string(log.GetHistory()), "unknown VRAM user: 200"); got != 0 {
		t.Errorf("warned about 200 MiB unknown")
	}
}

func TestTenants_NoMeasurementWithoutReader(t *testing.T) {
	m := New(reserveTenants(config.TenantOnBlockedHold), logmon.NewWriter(io.Discard))
	m.ConfigureVRAM(8000, VRAMConfig{ReserveMiB: 500})
	st := m.Status(nil, nil)
	if st.VRAM.MeasuredMiB != nil || st.VRAM.UnknownMiB != nil {
		t.Errorf("vram status %+v, want null measured and unknown", st.VRAM)
	}
}

func TestTenants_ReserveSkipsMeasuredCheckWhenSwapEvictsUndeclaredModel(t *testing.T) {
	m := New(nil, logmon.NewWriter(io.Discard))
	m.memMaxAge = 0
	m.Start(t.Context(), &fakeRouter{running: map[string]process.ProcessState{"mystery": process.StateReady}})
	gpu := &fakeReader{used: 7000}
	m.ConfigureVRAM(8000, VRAMConfig{ReserveMiB: 500, Models: map[string]int{"next": 1000}, Reader: gpu.read})

	// mystery declares no vram, so its 7000 MiB cannot be subtracted: the
	// measured check is skipped instead of refusing a load its swap makes room for.
	if reason, _ := m.Block(t.Context(), "next", nil); reason != nil {
		t.Fatalf("next blocked by the undeclared model its swap evicts: %v", reason)
	}
}

func TestTenants_WarnsAboutModelThatCanNeverLoad(t *testing.T) {
	log := logmon.NewWriter(io.Discard)
	m := New(nil, log)
	m.ConfigureVRAM(8000, VRAMConfig{ReserveMiB: 500, External: map[string]int{"qmd": 2000}, Models: map[string]int{"huge": 6000}})
	want := "tenants: model huge can never load: vram 6000 MiB plus vramReserve 500 MiB plus vramExternal 2000 MiB is 500 MiB over the 8000 MiB total"
	if history := string(log.GetHistory()); !strings.Contains(history, want) {
		t.Errorf("log lacks %q\nlog:\n%s", want, history)
	}
}
