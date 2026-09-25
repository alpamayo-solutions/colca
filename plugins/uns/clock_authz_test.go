package uns

import "testing"

func TestMountedLocalServiceReadsClockCoordination(t *testing.T) {
	local := &Entry{ULID: "connector", Kind: KindLocal, Name: "connector", Element: elementAt("line/machine")}
	for _, contract := range []string{"_ClockDefinition", "_ClockProgress", "_ServiceDetails"} {
		for _, action := range []Action{ActReadRecord, ActSub} {
			topic := "colca/v1/" + contract + "/node/other-worker/_service"
			if !Authorize(ns, local, action, topic) {
				t.Errorf("mounted local service cannot read %s via %v", contract, action)
			}
			for _, kind := range []Kind{KindExternal, KindHuman, KindNode} {
				outsider := *local
				outsider.Kind = kind
				if Authorize(ns, &outsider, action, topic) {
					t.Errorf("clock read widened %s via %v", kind, action)
				}
			}
		}
		if !Authorize(ns, local, ActSub, "colca/v1/"+contract+"/node/#") {
			t.Errorf("local alias subscription denied for %s", contract)
		}
		if Authorize(ns, local, ActPub, "colca/v1/"+contract+"/node/other-worker/_service") {
			t.Errorf("clock read widened publication of %s", contract)
		}
	}
	for _, topic := range []string{
		"colca/v1/_Metric/node/other-machine/temp",
		"colca/v1/_Annotation/node/other-machine/event",
		"colca/v1/_Group/node/operators",
		"colca/v1/+/node/#", "colca/#", "#",
	} {
		if Authorize(ns, local, ActSub, topic) {
			t.Errorf("clock read widened unrelated subscription %s", topic)
		}
	}
}
