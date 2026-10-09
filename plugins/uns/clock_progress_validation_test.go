package uns

import "testing"

func TestClockProgressRequiresExplicitFunctionalReadiness(t *testing.T) {
	for _, payload := range []string{
		`{"run_id":"run","processed_at":10,"ready":true,"observed_at":100}`,
		`{"run_id":"run","processed_at":10,"ready":false,"observed_at":null}`,
	} {
		if err := Validate("_ClockProgress", []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	for _, payload := range []string{
		`{"run_id":"run","processed_at":10}`,
		`{"run_id":"run","processed_at":10,"ready":true,"observed_at":null}`,
		`{"run_id":"run","processed_at":10,"ready":true}`,
		`{"run_id":"run","processed_at":10,"ready":"yes","observed_at":100}`,
		`{"run_id":"run","processed_at":10,"ready":false,"observed_at":"old"}`,
	} {
		if err := Validate("_ClockProgress", []byte(payload)); err == nil {
			t.Fatalf("accepted %s", payload)
		}
	}
}
