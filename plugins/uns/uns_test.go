package uns

import "testing"

func TestParseAndClass(t *testing.T) {
	p, err := Parse("colca/v1/_Metric/m1/site1/edge1/m1/temp")
	if err != nil {
		t.Fatal(err)
	}
	if p.Contract != "_Metric" || p.NodeID != "m1" || p.Path != "site1/edge1/m1/temp" {
		t.Fatalf("%+v", p)
	}
	if p.Prefix != "colca" || p.Version != "v1" {
		t.Fatalf("%+v", p)
	}
	cases := map[string]struct {
		class  Class
		stream string
	}{
		"_Metric": {ClassData, "metrics"}, "_EdgeNode": {ClassEntity, "entities"},
		"_SystemElement": {ClassEntity, "entities"}, "_CmdParam": {ClassCmd, "commands"},
		"_CmdAdmin": {ClassCmd, "commands"}, "_Ack": {ClassAck, "commands"},
		// demo topology: every _Cmd* contract is a command, never ClassNone
		"_CmdOperate": {ClassCmd, "commands"}, "_CmdMaintain": {ClassCmd, "commands"},
		"_Signal": {ClassEntity, "entities"},
	}
	for c, want := range cases {
		if ClassOf(c) != want.class || StreamFor(ClassOf(c)) != want.stream {
			t.Fatalf("%s → %v/%s", c, ClassOf(c), StreamFor(ClassOf(c)))
		}
	}
	if ClassOf("_Unknown") != ClassNone || StreamFor(ClassNone) != "" {
		t.Fatalf("unknown contract must be ClassNone with empty stream")
	}
	if _, err := Parse("colca/v1/nounderscore/m1/x"); err == nil {
		t.Fatal("want grammar error")
	}
	if _, err := Parse("colca/v1/_Metric/m1"); err == nil {
		t.Fatal("want error: missing path")
	}
	if IsUns("other/topic") {
		t.Fatal("non-UNS must be false")
	}
	if !IsUns("colca/v1/_Metric/m1/m1/temp") {
		t.Fatal("UNS topic must be true")
	}
}

func TestMountInsertStrip(t *testing.T) {
	in := "colca/v1/_Metric/m1/m1/temp"
	out := MountInsert(in, "edge1")
	if out != "colca/v1/_Metric/m1/edge1/m1/temp" {
		t.Fatal(out)
	}
	back, ok := MountStrip(out, "edge1")
	if !ok || back != in {
		t.Fatalf("%s %v", back, ok)
	}
	if _, ok := MountStrip("colca/v1/_Metric/m1/other/x", "edge1"); ok {
		t.Fatal("strip must fail for foreign mount")
	}

	// demo topology: 3-level chain machine → edge1 → site1 → global
	global := MountInsert(MountInsert(in, "edge1"), "site1")
	if global != "colca/v1/_Metric/m1/site1/edge1/m1/temp" {
		t.Fatal(global)
	}

	// downlink: mount-strip once per hop on the way down
	cmdGlobal := "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed"
	atSite, ok := MountStrip(cmdGlobal, "site1")
	if !ok || atSite != "colca/v1/_CmdParam/m1/edge1/m1/set-speed" {
		t.Fatalf("%s %v", atSite, ok)
	}
	atEdge, ok := MountStrip(atSite, "edge1")
	if !ok || atEdge != "colca/v1/_CmdParam/m1/m1/set-speed" {
		t.Fatalf("%s %v", atEdge, ok)
	}
	if _, ok := MountStrip(cmdGlobal, "edge1"); ok {
		t.Fatal("foreign mount must not be delivered")
	}
}

func TestValidate(t *testing.T) {
	ok := [][2]string{
		{"_Metric", `{"v": 3.14}`},
		{"_Metric", `{"v": 3.14, "ts": 123}`},
		{"_CmdParam", `{"correlation_id":"abc","expires_at": 99999999999, "params":{"speed":5}}`},
		{"_Ack", `{"correlation_id":"abc","result_code":200,"message":"ok"}`},
		{"_EdgeNode", `{"ulid":"n-edge1","mount":"edge1","typ":"node"}`},
	}
	for _, c := range ok {
		if err := Validate(c[0], []byte(c[1])); err != nil {
			t.Fatalf("%s should validate: %v", c[0], err)
		}
	}
	bad := [][2]string{
		{"_Metric", `{"v":"notanumber"}`},
		{"_Metric", `{}`},
		{"_CmdParam", `{"correlation_id":"abc"}`}, // missing expires_at
		{"_Ack", `{"result_code":200}`},           // missing correlation_id
		{"_Unknown", `{}`},                        // unknown contract
		{"_Metric", `not json`},
	}
	for _, c := range bad {
		if err := Validate(c[0], []byte(c[1])); err == nil {
			t.Fatalf("%s/%s must be rejected", c[0], c[1])
		}
	}
}
