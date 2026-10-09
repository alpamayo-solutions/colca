package historian

import (
	"encoding/json"
	"flag"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

// The Go half of the shared stored-timestamp vectors.
//
// A record's timestamp is unix seconds as a float. The historian turns it
// into the instant it stores in two steps: timestampOf rounds the fraction to
// nanoseconds, and the nanoseconds are cut to the microseconds Postgres keeps
// (pgx encodes timestamptz as Unix()*1e6 + Nanosecond()/1000; valueColumns
// mirrors that with Truncate(time.Microsecond)).
//
// PREKIT stores annotation times, and an annotation's bounds are often sample
// timestamps: it must land every float on the SAME microsecond, or the sample
// that opens an annotation lies one microsecond outside it. PREKIT's api
// carries a copy of the vector file and reproduces it bit for bit
// (prekit: api/src/utils/stream_time.py, api/src/utils/tests/test_stream_time.py,
// api/src/utils/tests/data/stored_timestamps.json). A change to this
// conversion fails this test here and that one there: change both, together.
//
// Regenerate with: go test ./internal/historian -run TestStoredTimestampVectors -update-timestamp-vectors
const timestampVectorPath = "testdata/stored_timestamps.json"

var updateTimestampVectors = flag.Bool("update-timestamp-vectors", false, "rewrite "+timestampVectorPath)

type timestampVectors struct {
	About string            `json:"about"`
	Cases []timestampVector `json:"cases"`
}

type timestampVector struct {
	// Why this input is here; empty for the bulk of random ones.
	Why string `json:"why,omitempty"`
	// The payload's timestamp as the JSON number a record carries.
	Seconds json.Number `json:"seconds"`
	// The instant the historian stores, in microseconds since the epoch.
	StoredUS int64 `json:"stored_us"`
}

// storedMicroseconds is what reaches the timestamptz column for a payload
// timestamp: timestampOf, then the driver's cut to microseconds.
func storedMicroseconds(seconds json.Number) int64 {
	at := timestampOf(&seconds, 0)
	return at.Unix()*1_000_000 + int64(at.Nanosecond())/1000
}

func TestStoredTimestampVectors(t *testing.T) {
	if *updateTimestampVectors {
		writeTimestampVectors(t)
	}
	raw, err := os.ReadFile(timestampVectorPath)
	if err != nil {
		t.Fatal(err)
	}
	var vectors timestampVectors
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors.Cases) < 3000 {
		t.Fatalf("%d cases; the file lost its bulk", len(vectors.Cases))
	}
	for _, c := range vectors.Cases {
		if got := storedMicroseconds(c.Seconds); got != c.StoredUS {
			t.Fatalf("seconds %s (%s): stored %d µs, the vectors say %d — PREKIT converts annotation "+
				"times with the same rule and must change with this (see the comment above)",
				c.Seconds, c.Why, got, c.StoredUS)
		}
		// The sink's own idea of "as Postgres stores it" agrees with the driver's.
		at := timestampOf(&c.Seconds, 0)
		if got := at.Truncate(time.Microsecond).UnixMicro(); got != c.StoredUS {
			t.Fatalf("seconds %s: Truncate gives %d µs, the driver %d", c.Seconds, got, c.StoredUS)
		}
	}
}

func writeTimestampVectors(t *testing.T) {
	t.Helper()
	random := rand.New(rand.NewSource(20261009))
	number := func(f float64) json.Number { return json.Number(strconv.FormatFloat(f, 'f', -1, 64)) }
	var cases []timestampVector
	add := func(why string, seconds json.Number) {
		cases = append(cases, timestampVector{Why: why, Seconds: seconds, StoredUS: storedMicroseconds(seconds)})
	}
	base := int64(1_767_225_600) // 2026-01-01T00:00:00Z
	year := int64(365 * 86400)

	for _, literal := range []string{
		"0", "0.0", "1", "0.5", "0.000001", "0.0000005", "0.0000004999", "0.0000015", "0.0000025",
		"0.9999999994", "0.9999999995", "0.9999999996", "0.999999", "0.9999995",
		"-0.000001", "-0.0000005", "-0.5", "-1", "-1.5", "-1767225600.25",
		"1767225600", "1767225600.0", "1767225600.000001", "1767225600.0000005", "1767225600.9999999",
		"1767225600.99999999", "1767225600.999999999", "1767225601", "1791476047.775",
		"1.7914760477750001e9", "1791476047775e-3", "4102444800.123456", "253402300799.999999",
	} {
		add("literal", json.Number(literal))
	}
	for i := 0; i < 1500; i++ {
		micros := base*1_000_000 + random.Int63n(year*1_000_000)
		add("a microsecond-exact instant as the nearest float", number(float64(micros)/1e6))
	}
	for i := 0; i < 300; i++ {
		millis := base*1000 + random.Int63n(year*1000)
		add("a millisecond-exact instant", number(float64(millis)/1e3))
	}
	for i := 0; i < 1200; i++ {
		add("an arbitrary float", number(float64(base)+random.Float64()*float64(year)))
	}
	for i := 0; i < 300; i++ {
		second := float64(base + random.Int63n(year))
		add("just below a second", number(math.Nextafter(second, math.Inf(-1))))
		add("a whole second", number(second))
		add("just above a second", number(math.Nextafter(second, math.Inf(1))))
	}
	for i := 0; i < 300; i++ {
		micros := base*1_000_000 + random.Int63n(year*1_000_000)
		add("half a microsecond past an instant", number((float64(micros)+0.5)/1e6))
		add("just short of the next microsecond", number(math.Nextafter(float64(micros+1)/1e6, math.Inf(-1))))
	}
	for i := 0; i < 200; i++ {
		add("before the epoch", number(-random.Float64()*float64(year)))
	}
	vectors := timestampVectors{
		About: "Unix seconds as a float -> the microsecond the historian stores (colca " +
			"internal/historian: timestampOf, then the cut to microseconds). Generated by running that " +
			"code: go test ./internal/historian -run TestStoredTimestampVectors -update-timestamp-vectors. " +
			"PREKIT's api keeps a copy and reproduces it (api/src/utils/tests/data/stored_timestamps.json).",
		Cases: cases,
	}
	raw, err := json.MarshalIndent(vectors, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(timestampVectorPath, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
