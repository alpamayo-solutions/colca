package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/alpamayo-solutions/colca/internal/identity"
)

// RunFootprint answers the "lightweight" claim with a number: RSS of the REAL
// colcad binary (not the in-process harness) as a standalone edge node — idle,
// then under one machine publishing flat out for Duration.
func RunFootprint(p Params) (*Report, error) {
	dir := p.WorkDir
	keyPath := filepath.Join(dir, "fp.key")
	if _, err := identity.Generate(keyPath); err != nil {
		return nil, err
	}
	// Fixed loopback ports: colcad has no way to report resolved :0 ports to
	// a parent process. High ports, bound only for the scenario's lifetime.
	const apiAddr, mqttAddr = "127.0.0.1:19301", "127.0.0.1:19302"
	cfg := fmt.Sprintf(`ulid: n-fp
data_dir: %s
key_file: %s
api:
  addr: %s
  token: %s
mqtt:
  addr: %s
`, filepath.Join(dir, "fp-data"), keyPath, apiAddr, BenchToken, mqttAddr)
	cfgPath := filepath.Join(dir, "fp.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		return nil, err
	}

	cmd := exec.Command(p.ColcadPath, cfgPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start colcad: %w", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	// Wait for /healthz, then let allocations settle before the idle sample.
	// A per-request timeout keeps a stalled connection from blocking past the
	// overall deadline — http.DefaultClient has no timeout of its own.
	hc := &http.Client{Timeout: 2 * time.Second, Transport: apiTransport()}
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := hc.Get("https://" + apiAddr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("colcad never became healthy on %s", apiAddr)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Enroll m1 through the REAL enrollment door (the binary has no other way
	// to learn an identity), then let allocations settle for the idle sample.
	m1id, err := identity.Generate(filepath.Join(dir, "m1.key"))
	if err != nil {
		return nil, err
	}
	entry, _ := json.Marshal(map[string]any{"ulid": "m1", "pubkey": m1id.PublicHex(), "kind": "machine", "mount": "m1"})
	req, err := http.NewRequest("POST", "https://"+apiAddr+"/enroll", bytes.NewReader(entry))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Colca-Token", BenchToken)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("enroll m1: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("enroll m1: HTTP %d", resp.StatusCode)
	}
	m1cert, err := m1id.SelfSignedCert("m1")
	if err != nil {
		return nil, err
	}
	time.Sleep(2 * time.Second)

	r := NewReport("footprint", p.Storage, map[string]any{"duration": p.Duration.String()})
	idle, err := RSSBytes(cmd.Process.Pid)
	if err != nil {
		return nil, err
	}
	r.Metrics["footprint_idle_mb"] = float64(idle) / (1 << 20)

	m, err := connect(mqttAddr, "m1", "m1", &benchIdentity{id: m1id, cert: m1cert})
	if err != nil {
		return nil, err
	}
	defer m.Disconnect(100)
	stopAt := time.Now().Add(p.Duration)
	seq := 0
	var peak uint64 = idle
	for time.Now().Before(stopAt) {
		seq++
		payload, _ := json.Marshal(map[string]any{"v": float64(seq)})
		tk := m.Publish("colca/v1/_Metric/m1/temp", 1, false, payload)
		if !tk.WaitTimeout(10*time.Second) || tk.Error() != nil {
			return nil, fmt.Errorf("publish under load: %w", tk.Error())
		}
		if seq%100 == 0 {
			if rss, err := RSSBytes(cmd.Process.Pid); err == nil && rss > peak {
				peak = rss
			}
		}
	}
	if rss, err := RSSBytes(cmd.Process.Pid); err == nil && rss > peak {
		peak = rss
	}
	r.Metrics["footprint_loaded_mb"] = float64(peak) / (1 << 20)
	return r, nil
}
