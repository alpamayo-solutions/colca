package mqttsrv

import (
	"errors"
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
)

// pubackReasonCodes are the reason codes MQTT 5 allows in a PUBACK (3.4.2.1). A
// client may reject any other code; paho's Python client crashes on one.
var pubackReasonCodes = map[byte]bool{
	0x00: true, 0x10: true, 0x80: true, 0x83: true, 0x87: true,
	0x90: true, 0x91: true, 0x97: true, 0x99: true,
}

func TestEveryEngineRefusalAnswersWithAValidPubackCode(t *testing.T) {
	cl := &mqtt.Client{}
	cl.Properties.ProtocolVersion = 5
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Qos: 1}}

	for _, reason := range []string{
		metrics.ReasonNodeID, metrics.ReasonGrammar, metrics.ReasonValidation, metrics.ReasonIdentity,
		metrics.ReasonWriteDenied, metrics.ReasonCmdDenied, metrics.ReasonRegistryContract,
		metrics.ReasonHumanWrite, metrics.ReasonTimeSync, metrics.ReasonDraining, "",
	} {
		var code packets.Code
		if err := rejectCode(cl, pk, &engine.RejectError{Reason: reason}); !errors.As(err, &code) {
			t.Fatalf("reason %q is answered with %v, not a reason code", reason, err)
		}
		if !pubackReasonCodes[code.Code] {
			t.Errorf("reason %q is answered with 0x%02X, which is not a PUBACK reason code", reason, code.Code)
		}
	}
}
