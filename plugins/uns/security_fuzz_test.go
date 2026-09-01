package uns

import "testing"

func FuzzGrantGrammar(f *testing.F) {
	for _, seed := range []string{
		"read:#",
		"write:01J00000000000000000000000/#",
		"cmd:01J00000000000000000000000/#:operate,maintain",
		"admin:#",
		"",
		"cmd:#:",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		grant, err := ParseGrant(raw)
		if err != nil {
			return
		}

		normalized, err := FormatGrant(grant)
		if err != nil {
			t.Fatalf("accepted grant cannot be formatted: %v", err)
		}
		reparsed, err := ParseGrant(normalized)
		if err != nil {
			t.Fatalf("formatted grant cannot be parsed: %v", err)
		}
		renormalized, err := FormatGrant(reparsed)
		if err != nil {
			t.Fatalf("reparsed grant cannot be formatted: %v", err)
		}
		if normalized != renormalized {
			t.Fatalf("grant normalization is unstable: %q != %q", normalized, renormalized)
		}
	})
}

func FuzzTopicAndPayloadGrammar(f *testing.F) {
	f.Add("colca/v1/_Metric/node/path", []byte(`{"v":1}`))
	f.Add("colca/v1/_CmdConfigure/node/path", []byte(`{"expires_at":1}`))
	f.Add("colca/v1/_TimeSync/node", []byte(`{"offset_ms":0}`))
	f.Add("", []byte{})

	f.Fuzz(func(t *testing.T, topic string, payload []byte) {
		parsed, err := Parse(topic)
		if err != nil {
			return
		}

		err = Validate(parsed.Contract, payload)
		if err == nil && !IsKnown(ClassOf(parsed.Contract)) {
			t.Fatalf("unknown contract %q accepted", parsed.Contract)
		}
	})
}
