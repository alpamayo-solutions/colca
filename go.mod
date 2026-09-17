module github.com/alpamayo-solutions/colca

go 1.26.8

require (
	github.com/cockroachdb/pebble/v2 v2.1.7
	github.com/eclipse/paho.golang v0.23.0
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/jackc/pgx/v5 v5.11.0
	github.com/mochi-mqtt/server/v2 v2.7.9
	github.com/oklog/ulid/v2 v2.1.2
	github.com/prometheus/client_golang v1.24.1
	github.com/prometheus/client_model v0.6.3
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3
	golang.org/x/crypto v0.57.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/RaduBerinde/axisds v0.1.0 // indirect
	github.com/RaduBerinde/btreemap v0.0.0-20250419174037-3d62b7205d54 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/crlib v0.0.0-20241112164430-1264a2edc35b // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/swiss v0.0.0-20260820225851-333444432258 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.5-0.20231225225746-43d5d4cd4e0e // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/minio/minlz v1.0.1-0.20250507153514-87eb42fe8882 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/rs/xid v1.4.0 // indirect
	golang.org/x/exp v0.0.0-20230626212559-97b1e661b5df // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

// Pinned to the org fork until upstream ships the retained-scan race fix:
// https://github.com/mochi-mqtt/server/pull/539 (mochi-mqtt/server#200).
// The fork also makes the Websocket listener bind at Init and report its bound
// address, so a ":0" door is held from the moment it is reported — upstream as
// https://github.com/mochi-mqtt/server/pull/542, beside #539. Its branch
// fix/flush-outbuf-before-close adds a flush of buffered writes before a client
// connection closes, so a PUBACK written behind queued deliveries is not lost at
// shutdown; upstream does not have that yet.
// Drop this replace once a released mochi version contains all three;
// internal/mqttsrv's TestRetainedDeliveryRacingAWildcardSubscribeDoesNotRace goes
// red under -race if it is dropped early, and
// TestAPubackBufferedBehindQueuedDeliveriesReachesTheClientBeforeShutdown fails
// without the flush.
replace github.com/mochi-mqtt/server/v2 => github.com/alpamayo-solutions/mochi-server/v2 v2.7.10-0.20260914070953-4d586594fc32
