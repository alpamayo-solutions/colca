// Package metrics is Colca's Prometheus surface: atomic counters bumped on the
// hot paths, and a collector that derives every gauge (stream offsets, cursor
// lag, child high-water marks, low-water marks, retention pressure) from the
// store at scrape time.
//
// Family names and labels are a contract that dashboards and alerts rely on.
// Every method is a no-op on a nil *Metrics, so packages stay testable without
// a registry.
package metrics

import (
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Reject reasons: the label values of colca_rejected_publishes_total.
const (
	ReasonNodeID      = "node_id"      // topic level 4 is not this node
	ReasonGrammar     = "grammar"      // topic does not parse as uns grammar
	ReasonValidation  = "validation"   // payload fails the contract's schema
	ReasonIdentity    = "identity"     // payload authorship contradicts the authenticated identity
	ReasonWriteDenied = "write_denied" // no write scope covers the topic
	ReasonCmdDenied   = "cmd_denied"   // a client's _Cmd* publish had no covering cmd grant
	// ReasonRegistryContract: an _EnrolledIdentity arrived at an ingest door.
	// Registry entries only enter through the enrollment endpoint.
	ReasonRegistryContract = "registry_contract"
	// ReasonHumanWrite: a person published data, entity or ack records. People
	// command; machines write state.
	ReasonHumanWrite = "human_write"
	// ReasonTimeSync: a _TimeSync publish came from a client, an admin caller or a
	// replicated batch. Only the node's own beacon may publish it.
	ReasonTimeSync = "time_sync"
	// ReasonDraining: a command targeted a mount that is being drained. New
	// commands are refused so the drain can finish.
	ReasonDraining = "draining"
)

var reasons = []string{ReasonNodeID, ReasonGrammar, ReasonValidation, ReasonIdentity, ReasonWriteDenied, ReasonCmdDenied, ReasonRegistryContract, ReasonHumanWrite, ReasonTimeSync, ReasonDraining}

// Move-drain outcomes: the label values of colca_drains_completed_total.
const (
	DrainOutcomeDelivered = "delivered" // the commands queue was already empty at completion
	DrainOutcomeExpired   = "expired"   // undelivered leftovers timed out (expires_at < now)
	DrainOutcomeForced    = "forced"    // DELETE /enroll/{ulid} interrupted an active drain
	// DrainOutcomeGapped: retention pruned some of a draining child's undelivered
	// commands, so "delivered" would not be true.
	DrainOutcomeGapped = "gapped"
)

var drainOutcomes = []string{DrainOutcomeDelivered, DrainOutcomeExpired, DrainOutcomeForced, DrainOutcomeGapped}

// Auth doors and rejection reasons: the labels of
// colca_auth_rejections_total{door,reason}. It counts identities turned away at
// a door; rejected publishes have their own family.
const (
	DoorMQTT = "mqtt"
	DoorHTTP = "http"
	DoorRepl = "repl"
	// DoorLocal is the plaintext local door. Reachability is the credential, so
	// rejections are rare (no name, or a name that collides with a keyed
	// identity), but they are counted like any other.
	DoorLocal = "local"
	// DoorHuman is the token door for people, over TCP or WebSocket. It labels
	// MQTT deliveries; its auth rejections count under DoorMQTT.
	DoorHuman = "human"

	AuthUnknownKey       = "unknown_key"       // TLS peer key not in the local registry (incl. revoked)
	AuthKind             = "kind"              // entry exists but its kind may not use this door
	AuthUsernameMismatch = "username_mismatch" // MQTT username != the key's enrolled ULID
	AuthToken            = "token"             // admin token missing or wrong
	AuthNoName           = "no_name"           // local door CONNECT carried no username
	AuthRegister         = "register"          // local door self-registration failed
	AuthMethod           = "auth_method"       // MQTT 5 authentication method this node does not speak
	AuthSubjectChanged   = "subject_changed"   // a re-authentication presented another person's token
)

var authDoors = []string{DoorMQTT, DoorHTTP, DoorRepl, DoorLocal}
var authReasons = []string{AuthUnknownKey, AuthKind, AuthUsernameMismatch, AuthToken, AuthNoName, AuthRegister,
	AuthMethod, AuthSubjectChanged}

// ACL denial actions: the labels of colca_acl_denials_total{action}. Only
// read-side denials are counted here (sub: MQTT subscribe, read: HTTP scope).
const (
	ACLSub  = "sub"
	ACLRead = "read"
)

var aclActions = []string{ACLSub, ACLRead}

// Security-change kinds are a bounded summary of successful commands that
// require an operator review. The children are pre-created so Prometheus sees
// the zero baseline and `increase()` detects the first event after startup.
const (
	SecurityChangeEnroll    = "enroll"
	SecurityChangeRevoke    = "revoke"
	SecurityChangeConfigure = "configure"
)

var securityChangeKinds = []string{
	SecurityChangeEnroll,
	SecurityChangeRevoke,
	SecurityChangeConfigure,
}

// streams is every persistent stream, taken from the store so the families
// below cannot miss a stream added later.
var streams = store.Streams()

// deliveryDoors label colca_mqtt_delivered_*: the MQTT listeners a subscriber
// can use.
var deliveryDoors = []string{DoorMQTT, DoorLocal, DoorHuman}

// uplinkStreams are the streams that replicate upward. definitions only flow
// down, so an uplink gauge for it would look like a broken uplink.
var uplinkStreams = uns.UplinkStreams()

// retentionStreams are the streams the retention policy applies to.
// definitions is never pruned by age or size.
var retentionStreams = []string{"metrics", "entities", "commands", "audit", "alarms", "annotations", "logs"}

// Blob transfer and ingress rejection label values.
var blobDirections = []string{"push", "pull", "receive"}
var blobResults = []string{"ok", "error"}
var blobRejectReasons = []string{"too_large", "digest_mismatch", "bad_digest"}
var recordRejectReasons = []string{"too_large"}

// resourceReadResults are the result labels of colca_resource_reads_total.
// "pending" means the blob has not replicated here yet and a retry may help;
// any other blob-store fault is "error".
var resourceReadResults = []string{"ok", "pending", "denied", "not_found", "error"}

// gapSurfaces are the surface labels of colca_gap_served_total: GET /fetch and
// GET /downlink.
var gapSurfaces = []string{"fetch", "downlink"}

// Metrics is the node's registry plus pre-created children for the hot paths,
// so labels are resolved once and never per message.
type Metrics struct {
	reg *prometheus.Registry

	ingest     *prometheus.CounterVec
	rejected   *prometheus.CounterVec
	uplinkOK   *prometheus.GaugeVec
	uplinkFail *prometheus.CounterVec
	// colca_uplink_refused_total: the parent answered and refused the batch (4xx).
	// That is a misconfiguration or a bug, not a network problem.
	uplinkRefused *prometheus.CounterVec
	downlinkOK    prometheus.Gauge
	downlinkFail  prometheus.Counter
	// colca_downlink_cursor_beyond_head_total: this node's command position is past
	// the parent's stream head, so it hears nothing until the head passes it.
	downlinkBeyondHead prometheus.Counter
	// colca_downlink_head_absent_total: the parent answered hello without a command
	// head, as older parents do, so no start position could be adopted.
	downlinkHeadAbsent prometheus.Counter
	reseed             prometheus.Gauge
	authReject         *prometheus.CounterVec
	aclDeny            *prometheus.CounterVec
	kicks              prometheus.Counter
	// colca_mqtt_publish_dropped_total: mochi dropped a publish because a client's
	// outbound queue was full. A retained replay on subscribe is the usual burst;
	// a non-zero value means delivered state went missing.
	publishDropped prometheus.Counter
	// Publishes the broker wrote to subscribers, by door, pre-created per door.
	deliveredMessages   *prometheus.CounterVec // colca_mqtt_delivered_messages_total{door}
	deliveredBytes      *prometheus.CounterVec // colca_mqtt_delivered_payload_bytes_total{door}
	deliveredMessagesBy map[string]prometheus.Counter
	deliveredBytesBy    map[string]prometheus.Counter
	// People on the token doors.
	humanSessions prometheus.Gauge   // colca_human_sessions
	jwksKeys      prometheus.Gauge   // colca_jwks_keys
	jwksFailures  prometheus.Counter // colca_jwks_refresh_failures_total

	// Remote administration.
	nodeCmds         *prometheus.CounterVec // colca_node_cmds_total{contract,verb,result}
	securityChanges  *prometheus.CounterVec // colca_security_changes_total{kind}
	securityChangeBy map[string]prometheus.Counter
	nodePrefix       *prometheus.GaugeVec // colca_node_prefix_info{prefix}

	// colca_command_undelivered_total: a live command for a machine enrolled here
	// found no subscriber on the local bus. It stays durable and is replayed when
	// the machine subscribes (commandRedelivered). Commands on their way to a
	// descendant are not counted. It checks subscriptions, not bytes, so it can
	// undercount but never overcount.
	commandUndelivered prometheus.Counter

	// colca_command_unroutable_total: a command for another node was stored while
	// no enrolled child's mount covered its path, so nothing will hand it down.
	// Expiry is decided at the target, so such a command would otherwise sit
	// silently forever. It only counts: refusing would break publishing before
	// enrollment and reparenting.
	commandUnroutable prometheus.Counter

	// colca_command_redelivered_total: a stored command was replayed onto the local
	// bus because its machine subscribed with its delivery cursor still before it.
	commandRedelivered prometheus.Counter

	// Contracts bundle.
	bundleInfo      *prometheus.GaugeVec // colca_contracts_bundle_info{version,digest,source}
	bundleContracts prometheus.Gauge     // colca_contracts_bundle_contracts

	// Retention pruner.
	prunedRecords *prometheus.CounterVec // colca_retention_pruned_records_total{stream}
	prunedBytes   *prometheus.CounterVec // colca_retention_pruned_bytes_total{stream}
	pruneRuns     *prometheus.CounterVec // colca_retention_prune_runs_total{stream}
	gapRecords    *prometheus.CounterVec // colca_retention_gap_records_total{stream}
	// The state refresh only runs on the entities stream, so these are unlabeled.
	// A failed refresh append is internal repair, not a rejected publish.
	refreshRecords  prometheus.Counter // colca_retention_state_refresh_records_total
	refreshSkipped  prometheus.Counter // colca_retention_state_refresh_skipped_total
	refreshFailures prometheus.Counter // colca_retention_state_refresh_failures_total

	// Stream gaps.
	gapServed      *prometheus.CounterVec // colca_gap_served_total{stream,surface}
	gapReceived    *prometheus.CounterVec // colca_gap_received_total{stream}
	replGapApplied *prometheus.CounterVec // colca_repl_gap_applied_total{child,stream}
	// Unlabelled and pre-created so an alert sees the first second-net gap;
	// the detailed family above remains the source for child/stream diagnosis.
	replIntegrityFailures prometheus.Counter // colca_replication_integrity_failures_total

	// Move-drain.
	drainsActive         prometheus.Gauge       // colca_drains_active
	drainPendingCommands *prometheus.GaugeVec   // colca_drain_pending_commands{child}
	drainsCompleted      *prometheus.CounterVec // colca_drains_completed_total{outcome}
	definitionsApplied   prometheus.Counter     // colca_definitions_applied_total
	definitionsRejected  prometheus.Counter     // colca_definitions_rejected_total
	auditWriteFailures   prometheus.Counter     // colca_audit_write_failures_total
	drainsCompletedBy    map[string]prometheus.Counter

	// Blob transfers and ingress rejections.
	blobTransfers   *prometheus.CounterVec // colca_blob_transfers_total{direction,result}
	blobTransfersBy map[string]prometheus.Counter
	blobRejects     *prometheus.CounterVec // colca_blob_rejects_total{reason}
	blobRejectsBy   map[string]prometheus.Counter
	recordRejects   *prometheus.CounterVec // colca_record_rejects_total{reason}
	recordRejectsBy map[string]prometheus.Counter

	// Resource file reads on the published door.
	resourceReads   *prometheus.CounterVec // colca_resource_reads_total{result}
	resourceReadsBy map[string]prometheus.Counter

	// Rate and concurrency limits. Both labels are fixed door and route classes,
	// never paths or identities.
	httpRequestLimited *prometheus.CounterVec // colca_http_request_limited_total{door,class}

	// Per-caller read load, to see who spends the scan and fetch budgets. The
	// caller label is a registered identity (kind:name), "human" for every
	// person, or "admin"; route, contract, stream and depth labels come from
	// fixed sets, never from raw paths.
	httpKVRequests      *prometheus.CounterVec   // colca_http_kv_requests_total{caller,contract,prefix_depth}
	httpKVEntries       *prometheus.HistogramVec // colca_http_kv_entries{caller}
	httpFetchRequests   *prometheus.CounterVec   // colca_http_fetch_requests_total{caller,stream}
	httpLimitedByCaller *prometheus.CounterVec   // colca_http_request_limited_by_caller_total{route,caller}
	// The (route, caller) pairs whose limited series already exists, so a
	// dashboard sees 0 before the first 429.
	httpCallersSeen sync.Map

	// Blob sweeper: unreferenced blobs reclaimed after their grace period.
	blobsSwept prometheus.Counter // colca_blobs_swept_total

	// colca_metrics_unbound_total: a _Metric accepted on a path with no _Signal.
	// Unlabeled because paths are unbounded; a rate-limited log line names them.
	metricsUnbound prometheus.Counter // colca_metrics_unbound_total

	ingestBy        map[string]prometheus.Counter
	rejectedBy      map[string]prometheus.Counter
	uplinkOKBy      map[string]prometheus.Gauge
	uplinkFailBy    map[string]prometheus.Counter
	uplinkRefusedBy map[string]prometheus.Counter
	aclDenyBy       map[string]prometheus.Counter
	prunedRecordsBy map[string]prometheus.Counter
	prunedBytesBy   map[string]prometheus.Counter
	pruneRunsBy     map[string]prometheus.Counter
	gapRecordsBy    map[string]prometheus.Counter
	gapServedBy     map[string]map[string]prometheus.Counter // [stream][surface]
	gapReceivedBy   map[string]prometheus.Counter
}

// New builds the registry with every family pre-created, so all of them are
// present from the first scrape. cfg is the retention policy the collector
// needs for the pressure and blocked-by-cursor gauges.
//
// clk must be the same *clock.Clock the engine uses, or the clock gauges
// report state nobody updates. A nil clk reads as never synced.
func New(st *store.Store, cfg config.Retention, clk *clock.Clock) *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		ingest: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_ingest_records_total",
			Help: "Records durably persisted, by stream. Resets on restart.",
		}, []string{"stream"}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_rejected_publishes_total",
			Help: "Publishes rejected before persistence, by reason. Resets on restart.",
		}, []string{"reason"}),
		uplinkOK: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "colca_uplink_last_success_timestamp_seconds",
			Help: "Unix time of the last successful uplink push+ack, by stream (0 = never this process).",
		}, []string{"stream"}),
		uplinkFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_uplink_push_failures_total",
			Help: "Failed uplink pushes, by stream. Resets on restart.",
		}, []string{"stream"}),
		uplinkRefused: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_uplink_refused_total",
			Help: "Uplink pushes the parent ANSWERED and refused (4xx), by stream — a misconfiguration or a bug between the two nodes, not an outage. Nothing is dropped: the batch is held and retried, so this counter rising is a lane that is not moving. Resets on restart.",
		}, []string{"stream"}),
		downlinkOK: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_downlink_last_success_timestamp_seconds",
			Help: "Unix time of the last successful downlink fetch, empty fetches included (0 = never this process).",
		}),
		downlinkFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_downlink_fetch_failures_total",
			Help: "Failed downlink fetches. Resets on restart.",
		}),
		downlinkBeyondHead: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_downlink_cursor_beyond_head_total",
			Help: "Starts at which this node's command cursor was already past its parent's stream head (parent pruned past it, or was rebuilt). Resets on restart.",
		}),
		downlinkHeadAbsent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_downlink_head_absent_total",
			Help: "Process starts whose parent answered hello without a command head (a parent predating parent-scoped cursors). A node with no commands cursor for that parent yet starts at 1 and may be handed commands issued under its mount before it attached; one that already has a cursor keeps it and loses nothing. Resets on restart.",
		}),
		reseed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_retained_reseed_records",
			Help: "KV entries replayed into the broker's retained set at startup.",
		}),
		authReject: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_auth_rejections_total",
			Help: "Identities turned away at a door, by door and reason. Resets on restart.",
		}, []string{"door", "reason"}),
		aclDeny: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_acl_denials_total",
			Help: "Read-side authorization denials (sub = MQTT subscribe, read = HTTP scope). Resets on restart.",
		}, []string{"action"}),
		kicks: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_session_kicks_total",
			Help: "Live MQTT sessions disconnected by a registry change or token expiry. Resets on restart.",
		}),
		publishDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_mqtt_publish_dropped_total",
			Help: "Publishes the broker dropped because a client's outbound queue was full (MaximumClientWritesPending). Resets on restart.",
		}),
		deliveredMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_mqtt_delivered_messages_total",
			Help: "PUBLISH packets the broker wrote to subscribers, by door (mqtt, local, human). Resets on restart.",
		}, []string{"door"}),
		deliveredBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_mqtt_delivered_payload_bytes_total",
			Help: "Payload bytes of the PUBLISH packets the broker wrote to subscribers, by door (mqtt, local, human). Resets on restart.",
		}, []string{"door"}),
		humanSessions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_human_sessions",
			Help: "Live token-authenticated MQTT sessions on the human doors.",
		}),
		jwksKeys: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_jwks_keys",
			Help: "Issuer signing keys currently cached (0 on a node with an auth block is alertable).",
		}),
		jwksFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_jwks_refresh_failures_total",
			Help: "Failed JWKS fetches (cached keys keep serving). Resets on restart.",
		}),
		nodeCmds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_node_cmds_total",
			Help: "Commands executed BY this node (as opposed to riding through to a machine), by contract, verb and outcome. Resets on restart.",
		}, []string{"contract", "verb", "result"}),
		securityChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_security_changes_total",
			Help: "Successful enrollment, revocation and node-configuration changes requiring operator review, by bounded kind. Resets on restart.",
		}, []string{"kind"}),
		nodePrefix: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "colca_node_prefix_info",
			Help: "The node's root-frame prefix as taught by its parent (info gauge, value 1; absent until learned).",
		}, []string{"prefix"}),
		commandUndelivered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_command_undelivered_total",
			Help: "Live commands addressed to a machine enrolled at THIS node, published to its local MQTT bus with zero live subscriptions at that moment (that machine was not connected). Commands transiting toward a descendant, or addressed to a child node, are excluded — those are delivered over replication and reach no local subscriber by design. Subscription existence, not byte-level delivery confirmation — can undercount a delivery that failed after a write to a live subscriber, never overcounts. The record is still durable in the commands stream — this counts a delivery attempt reaching nobody, not data loss. Resets on restart.",
		}),
		commandUnroutable: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_command_unroutable_total",
			Help: "Commands addressed to another node and persisted at admission while no enrolled child node's mount covered their path — nothing will ever hand them down, execute them or ack them, and they never expire visibly because expiry is evaluated at the target. Excludes commands for this node (executed in-process), for a machine enrolled here (see colca_command_undelivered_total) and for a draining child (a drain delivers what is queued). Observability only: nothing is refused on this, because refusing would break publish-before-enroll and every reparent window. Resets on restart.",
		}),
		commandRedelivered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_command_redelivered_total",
			Help: "Commands replayed from the durable commands stream onto the local MQTT bus when the machine they address subscribed with its delivery cursor still standing before them. The recovery half of colca_command_undelivered_total: these are deliveries that a broker restart, or an issue-before-first-connect, would otherwise have dropped. Resets on restart.",
		}),
		bundleInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "colca_contracts_bundle_info",
			Help: "Identity of the loaded contracts bundle (info gauge, value 1). source=builtin means the floor rules apply — the one-glance answer to which rules this node enforces.",
		}, []string{"version", "digest", "source"}),
		bundleContracts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_contracts_bundle_contracts",
			Help: "Number of contracts the loaded bundle carries (0 under the builtin floor).",
		}),
		prunedRecords: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_retention_pruned_records_total",
			Help: "Records removed by the retention pruner, by stream. Resets on restart.",
		}, []string{"stream"}),
		prunedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_retention_pruned_bytes_total",
			Help: "Logical bytes removed by the retention pruner, by stream. Resets on restart.",
		}, []string{"stream"}),
		pruneRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_retention_prune_runs_total",
			Help: "Prune cycles that actually removed at least one record, by stream. Resets on restart.",
		}, []string{"stream"}),
		gapRecords: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_retention_gap_records_total",
			Help: "Durable _StreamGap records emitted by the pruner, by stream. Resets on restart.",
		}, []string{"stream"}),
		refreshRecords: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_retention_state_refresh_records_total",
			Help: "KV entries re-appended to entities by the state refresh. Resets on restart.",
		}),
		refreshSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_retention_state_refresh_skipped_total",
			Help: "State-refresh appends skipped because the snapshot was superseded by a tombstone or a newer write. Resets on restart.",
		}),
		refreshFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_retention_state_refresh_failures_total",
			Help: "State-refresh appends that failed and left the refresh obligation pending. Resets on restart.",
		}),
		gapServed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_gap_served_total",
			Help: "Wire gaps served to a consumer, by stream and surface (fetch|downlink). Resets on restart.",
		}, []string{"stream", "surface"}),
		gapReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_gap_received_total",
			Help: "Gaps a repl client hit — uplink LWM jump or downlink gap object — by stream. Resets on restart.",
		}, []string{"stream"}),
		replGapApplied: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_repl_gap_applied_total",
			Help: "Child-offset jumps observed in ApplyReplicated, by child and stream. Resets on restart.",
		}, []string{"child", "stream"}),
		replIntegrityFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_replication_integrity_failures_total",
			Help: "Child-offset jumps observed in ApplyReplicated, summarized without dynamic labels so the first event is alertable. Resets on restart.",
		}),
		drainsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_drains_active",
			Help: "Enrolled kind=node children currently draining before a move.",
		}),
		drainPendingCommands: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "colca_drain_pending_commands",
			Help: "Live, undelivered ClassCmd records still blocking a draining child's completion, by child.",
		}, []string{"child"}),
		drainsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_drains_completed_total",
			Help: "Move-drains that reached a terminal outcome, by outcome: delivered (queue empty), expired (leftovers timed out), forced (DELETE during drain). Resets on restart.",
		}, []string{"outcome"}),
		definitionsApplied: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_definitions_applied_total",
			Help: "Definitions handed down by the parent and applied here as state. Resets on restart; the current set is the KV view, not this counter.",
		}),
		definitionsRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_definitions_rejected_total",
			Help: "Definitions the parent handed down that this node refused (bad grammar, wrong class, failed validation). Non-zero means policy or types are NOT arriving and the node's cursor is parked on the offending record — always worth an alert.",
		}),
		auditWriteFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_audit_write_failures_total",
			Help: "Security audit events that could not be durably appended. The protected operation remains denied. Resets on restart.",
		}),
		blobTransfers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_blob_transfers_total",
			Help: "Blob transfers by direction (push to parent, pull from parent, receive from child) and outcome. Resets on restart.",
		}, []string{"direction", "result"}),
		blobRejects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_blob_rejects_total",
			Help: "Blobs refused at ingress, by reason. Resets on restart.",
		}, []string{"reason"}),
		recordRejects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_record_rejects_total",
			Help: "Records refused at ingress for exceeding the configured size cap. Resets on restart.",
		}, []string{"reason"}),
		resourceReads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_resource_reads_total",
			Help: "Resource file reads on the published door's GET /resources/{id}/file, by result: ok (bytes served), pending (blob_pending — the record exists but its bytes have not replicated here, the only retryable case), denied (no read grant on the resource's element), not_found (unknown resource id), error (an internal fault reading the blob — a malformed stored digest or a disk/permission fault on this node; never retryable the way pending is). Resets on restart.",
		}, []string{"result"}),
		httpRequestLimited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_http_request_limited_total",
			Help: "HTTP requests refused by Colca's rate or concurrency controls, by bounded door and route class. Resets on restart.",
		}, []string{"door", "class"}),
		httpKVRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_http_kv_requests_total",
			Help: "GET /kv pages served, by caller, contract filter (one contract, \"multiple\" or \"all\") and the number of segments in the prefix (0 is the whole node, capped at \"5+\"). Resets on restart.",
		}, []string{"caller", "contract", "prefix_depth"}),
		httpKVEntries: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "colca_http_kv_entries",
			Help:    "Entries returned by one GET /kv page, by caller.",
			Buckets: []float64{0, 1, 10, 100, 1000, 10000},
		}, []string{"caller"}),
		httpFetchRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_http_fetch_requests_total",
			Help: "GET /fetch pages served, by caller and stream. Resets on restart.",
		}, []string{"caller", "stream"}),
		httpLimitedByCaller: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_http_request_limited_by_caller_total",
			Help: "HTTP requests answered 429 by the per-caller limits, by route pattern and caller. Resets on restart.",
		}, []string{"route", "caller"}),
		blobsSwept: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_blobs_swept_total",
			Help: "Blobs deleted by the background sweeper because no live _Resource referenced them and they were older than the configured grace period. Resets on restart.",
		}),
		metricsUnbound: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_metrics_unbound_total",
			Help: "_Metric records accepted on a path with no _Signal at that path: a valid, authorized write that does not appear as a signal until something binds that path. Accompanied by a rate-limited log line naming the path. Resets on restart.",
		}),
	}
	m.ingestBy = counterChildren(m.ingest, streams)
	m.deliveredMessagesBy = counterChildren(m.deliveredMessages, deliveryDoors)
	m.deliveredBytesBy = counterChildren(m.deliveredBytes, deliveryDoors)
	m.rejectedBy = counterChildren(m.rejected, reasons)
	m.uplinkFailBy = counterChildren(m.uplinkFail, uplinkStreams)
	m.uplinkRefusedBy = counterChildren(m.uplinkRefused, uplinkStreams)
	m.aclDenyBy = counterChildren(m.aclDeny, aclActions)
	m.securityChangeBy = counterChildren(m.securityChanges, securityChangeKinds)
	m.uplinkOKBy = make(map[string]prometheus.Gauge, len(uplinkStreams))
	for _, s := range uplinkStreams {
		m.uplinkOKBy[s] = m.uplinkOK.WithLabelValues(s)
	}
	m.prunedRecordsBy = counterChildren(m.prunedRecords, retentionStreams)
	m.prunedBytesBy = counterChildren(m.prunedBytes, retentionStreams)
	m.pruneRunsBy = counterChildren(m.pruneRuns, retentionStreams)
	m.gapRecordsBy = counterChildren(m.gapRecords, retentionStreams)
	m.gapReceivedBy = counterChildren(m.gapReceived, streams)
	m.gapServedBy = make(map[string]map[string]prometheus.Counter, len(streams))
	for _, s := range streams {
		byName := make(map[string]prometheus.Counter, len(gapSurfaces))
		for _, surface := range gapSurfaces {
			byName[surface] = m.gapServed.WithLabelValues(s, surface)
		}
		m.gapServedBy[s] = byName
	}
	// Pre-create every door×reason child so all label combinations scrape as 0.
	for _, d := range authDoors {
		for _, r := range authReasons {
			m.authReject.WithLabelValues(d, r)
		}
	}
	m.drainsCompletedBy = counterChildren(m.drainsCompleted, drainOutcomes)

	m.blobTransfersBy = map[string]prometheus.Counter{}
	for _, d := range blobDirections {
		for _, res := range blobResults {
			m.blobTransfersBy[d+"|"+res] = m.blobTransfers.WithLabelValues(d, res)
		}
	}
	m.blobRejectsBy = counterChildren(m.blobRejects, blobRejectReasons)
	m.recordRejectsBy = counterChildren(m.recordRejects, recordRejectReasons)
	m.resourceReadsBy = counterChildren(m.resourceReads, resourceReadResults)

	// A restart must not reset colca_drains_active while children are still
	// draining, so it starts from the entries persisted as draining.
	draining := 0
	persisted, err := st.RegistryScan()
	if err != nil {
		slog.Warn("metrics: cannot count draining entries", "err", err)
	}
	for _, raw := range persisted {
		// Decode the real entry so the rule for draining has one owner.
		var e uns.Entry
		if err := json.Unmarshal(raw, &e); err == nil && e.IsDraining() {
			draining++
		}
	}
	m.drainsActive.Set(float64(draining))

	// The clock gauges are read at scrape time: the sync age is seconds since the
	// last sample, now, which no write path could push ahead of time.
	clockOffset := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "colca_clock_offset_ms",
		Help: "Current authoritative-time offset estimate in milliseconds: offset_ms = now_ms - wall_receipt from the most recent /downlink or /replicate response. Always 0 on the root and on a node that has never synced.",
	}, func() float64 {
		if clk == nil {
			return 0
		}
		return float64(clk.OffsetMS())
	})
	clockSyncAge := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "colca_clock_sync_age_seconds",
		Help: "Seconds since the last accepted offset sample. The root exports 0 by definition. +Inf means never synced.",
	}, func() float64 {
		if clk == nil {
			return math.Inf(1)
		}
		// clk.Now() keeps tests with a fake clock deterministic.
		return clk.SyncAgeSeconds(clk.Now())
	})

	m.reg.MustRegister(m.ingest, m.rejected, m.uplinkOK, m.uplinkFail, m.uplinkRefused,
		m.downlinkOK, m.downlinkFail, m.downlinkBeyondHead, m.downlinkHeadAbsent, m.reseed,
		m.authReject, m.aclDeny, m.kicks, m.publishDropped, m.deliveredMessages, m.deliveredBytes, m.humanSessions, m.jwksKeys, m.jwksFailures,
		m.nodeCmds, m.securityChanges, m.nodePrefix,
		m.commandUndelivered, m.commandUnroutable, m.commandRedelivered,
		m.bundleInfo, m.bundleContracts,
		m.prunedRecords, m.prunedBytes, m.pruneRuns, m.gapRecords,
		m.refreshRecords, m.refreshSkipped, m.refreshFailures,
		m.gapServed, m.gapReceived, m.replGapApplied, m.replIntegrityFailures,
		m.drainsActive, m.drainPendingCommands, m.drainsCompleted,
		m.definitionsApplied, m.definitionsRejected, m.auditWriteFailures,
		m.blobTransfers, m.blobRejects, m.recordRejects, m.resourceReads, m.httpRequestLimited, m.blobsSwept,
		m.httpKVRequests, m.httpKVEntries, m.httpFetchRequests, m.httpLimitedByCaller,
		m.metricsUnbound,
		clockOffset, clockSyncAge,
		newStoreCollector(st, cfg, store.DefaultPolicyScanCap))
	return m
}

// AuditWriteFailure counts an internal security event that could not be
// persisted. It is deliberately separate from rejected publishes: the audit
// append is internal evidence, not another attempt at the protected action.
func (m *Metrics) AuditWriteFailure() {
	if m != nil {
		m.auditWriteFailures.Inc()
	}
}

// AuthReject counts one identity turned away at a door.
func (m *Metrics) AuthReject(door, reason string) {
	if m == nil {
		return
	}
	m.authReject.WithLabelValues(door, reason).Inc()
}

// ACLDeny counts one read-side authorization denial.
func (m *Metrics) ACLDeny(action string) {
	if m == nil {
		return
	}
	if c, ok := m.aclDenyBy[action]; ok {
		c.Inc()
		return
	}
	m.aclDeny.WithLabelValues(action).Inc()
}

// SetHumanSessions reports the number of live sessions on the token doors.
func (m *Metrics) SetHumanSessions(n int) {
	if m == nil {
		return
	}
	m.humanSessions.Set(float64(n))
}

// SetJWKSKeys reports how many issuer signing keys are cached (tokenauth.Metrics).
func (m *Metrics) SetJWKSKeys(n int) {
	if m == nil {
		return
	}
	m.jwksKeys.Set(float64(n))
}

// NodeCmd counts one command executed by this node, by contract, verb and
// outcome (ok, conflict, invalid, expired, error, blob_unreachable).
// blob_unreachable means the resource's blob could not be pulled, which is
// fixed by staging bytes rather than by changing the command. Commands for
// other nodes are counted where they execute.
func (m *Metrics) NodeCmd(contract, verb, result string) {
	if m == nil {
		return
	}
	m.nodeCmds.WithLabelValues(contract, verb, result).Inc()
	if result != "ok" {
		return
	}
	if contract == "_CmdAdmin" && (verb == SecurityChangeEnroll || verb == SecurityChangeRevoke) {
		m.securityChangeBy[verb].Inc()
	}
	if contract == "_CmdConfigure" {
		m.securityChangeBy[SecurityChangeConfigure].Inc()
	}
}

// CommandUndelivered counts one live command that reached no subscriber on the
// local bus.
func (m *Metrics) CommandUndelivered() {
	if m == nil {
		return
	}
	m.commandUndelivered.Inc()
}

// CommandUnroutable counts one command for a node no enrolled child's mount
// covers.
func (m *Metrics) CommandUnroutable() {
	if m == nil {
		return
	}
	m.commandUnroutable.Inc()
}

// CommandRedelivered counts one command replayed from the commands stream onto
// the local bus.
func (m *Metrics) CommandRedelivered() {
	if m == nil {
		return
	}
	m.commandRedelivered.Inc()
}

// SetBundleInfo reports the loaded contracts bundle, or source=builtin when only
// the built-in rules apply.
func (m *Metrics) SetBundleInfo(version, digest, source string, contracts int) {
	if m == nil {
		return
	}
	m.bundleInfo.Reset()
	m.bundleInfo.WithLabelValues(version, digest, source).Set(1)
	m.bundleContracts.Set(float64(contracts))
}

// SetNodePrefix reports the currently taught root-frame prefix as an info
// gauge: exactly one label value carries 1, older values are dropped.
func (m *Metrics) SetNodePrefix(p string) {
	if m == nil {
		return
	}
	m.nodePrefix.Reset()
	m.nodePrefix.WithLabelValues(p).Set(1)
}

// JWKSRefreshFailed counts one failed JWKS fetch (tokenauth.Metrics).
func (m *Metrics) JWKSRefreshFailed() {
	if m == nil {
		return
	}
	m.jwksFailures.Inc()
}

// SessionKick counts one live session disconnected by a registry change.
func (m *Metrics) SessionKick() {
	if m == nil {
		return
	}
	m.kicks.Inc()
}

// PublishDropped counts one publish mochi dropped because the client's outbound
// queue was full.
func (m *Metrics) PublishDropped() {
	if m == nil {
		return
	}
	m.publishDropped.Inc()
}

// MQTTDelivered counts one PUBLISH written to a subscriber on door and its
// payload size. door is one of DoorMQTT, DoorLocal and DoorHuman.
func (m *Metrics) MQTTDelivered(door string, payloadBytes int) {
	if m == nil {
		return
	}
	if c, ok := m.deliveredMessagesBy[door]; ok {
		c.Inc()
		m.deliveredBytesBy[door].Add(float64(payloadBytes))
	}
}

// counterChildren pre-resolves one child per known label value, so incrementing
// is a plain map hit — no per-message label allocation or vec lookup.
func counterChildren(vec *prometheus.CounterVec, labels []string) map[string]prometheus.Counter {
	out := make(map[string]prometheus.Counter, len(labels))
	for _, l := range labels {
		out[l] = vec.WithLabelValues(l)
	}
	return out
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// IngestRecord counts one durably persisted record on a stream.
func (m *Metrics) IngestRecord(stream string) {
	if m == nil {
		return
	}
	if c, ok := m.ingestBy[stream]; ok {
		c.Inc()
		return
	}
	m.ingest.WithLabelValues(stream).Inc()
}

// RejectPublish counts one rejected publish. reason should be one of the
// Reason* constants.
func (m *Metrics) RejectPublish(reason string) {
	if m == nil {
		return
	}
	if c, ok := m.rejectedBy[reason]; ok {
		c.Inc()
		return
	}
	m.rejected.WithLabelValues(reason).Inc()
}

// UplinkPushed records a successful uplink push+ack cycle for a stream.
func (m *Metrics) UplinkPushed(stream string, at time.Time) {
	if m == nil {
		return
	}
	g, ok := m.uplinkOKBy[stream]
	if !ok {
		g = m.uplinkOK.WithLabelValues(stream)
	}
	g.Set(float64(at.UnixNano()) / 1e9)
}

// UplinkPushFailed counts one failed uplink push for a stream.
func (m *Metrics) UplinkPushFailed(stream string) {
	if m == nil {
		return
	}
	if c, ok := m.uplinkFailBy[stream]; ok {
		c.Inc()
		return
	}
	m.uplinkFail.WithLabelValues(stream).Inc()
}

// UplinkRefused counts one uplink push the parent answered and refused. It is
// counted in addition to UplinkPushFailed and tells a refusal from an outage.
func (m *Metrics) UplinkRefused(stream string) {
	if m == nil {
		return
	}
	if c, ok := m.uplinkRefusedBy[stream]; ok {
		c.Inc()
		return
	}
	m.uplinkRefused.WithLabelValues(stream).Inc()
}

// DownlinkFetched records a successful downlink fetch. Empty fetches count too:
// they show the loop is alive.
func (m *Metrics) DownlinkFetched(at time.Time) {
	if m == nil {
		return
	}
	m.downlinkOK.Set(float64(at.UnixNano()) / 1e9)
}

// DownlinkFetchFailed counts one failed downlink fetch.
func (m *Metrics) DownlinkFetchFailed() {
	if m == nil {
		return
	}
	m.downlinkFail.Inc()
}

// DownlinkCursorBeyondHead counts one start whose command cursor was already
// past the parent's stream head, because the parent pruned past it or was
// rebuilt. Worth an alert: such a node receives no commands and cannot be
// repaired remotely.
func (m *Metrics) DownlinkCursorBeyondHead() {
	if m == nil {
		return
	}
	m.downlinkBeyondHead.Inc()
}

// DownlinkHeadAbsent counts one start whose parent answered hello without a
// command head, as an older parent does during a rolling upgrade. It rises on
// every start against such a parent, harmless or not. The case to alert on is
// a node without a cursor for that parent: it starts at 1 and receives commands
// issued under its mount before it attached.
func (m *Metrics) DownlinkHeadAbsent() {
	if m == nil {
		return
	}
	m.downlinkHeadAbsent.Inc()
}

// SetReseedCount records how many KV entries the startup reseed replayed.
func (m *Metrics) SetReseedCount(n int) {
	if m == nil {
		return
	}
	m.reseed.Set(float64(n))
}

// RetentionPruneRun counts one prune cycle that removed at least one record.
func (m *Metrics) RetentionPruneRun(stream string) {
	if m == nil {
		return
	}
	if c, ok := m.pruneRunsBy[stream]; ok {
		c.Inc()
		return
	}
	m.pruneRuns.WithLabelValues(stream).Inc()
}

// RetentionPruned adds one run's removed records and bytes. Pass the store's
// own post-commit figures: its in-batch cursor recheck can shrink the range
// after the caller's scan.
func (m *Metrics) RetentionPruned(stream string, records, bytes uint64) {
	if m == nil {
		return
	}
	if c, ok := m.prunedRecordsBy[stream]; ok {
		c.Add(float64(records))
	} else {
		m.prunedRecords.WithLabelValues(stream).Add(float64(records))
	}
	if c, ok := m.prunedBytesBy[stream]; ok {
		c.Add(float64(bytes))
	} else {
		m.prunedBytes.WithLabelValues(stream).Add(float64(bytes))
	}
}

// RetentionGapRecorded counts one durable _StreamGap record for stream.
func (m *Metrics) RetentionGapRecorded(stream string) {
	if m == nil {
		return
	}
	if c, ok := m.gapRecordsBy[stream]; ok {
		c.Inc()
		return
	}
	m.gapRecords.WithLabelValues(stream).Inc()
}

// StateRefreshApplied counts one entities entry re-appended by the state refresh.
func (m *Metrics) StateRefreshApplied() {
	if m == nil {
		return
	}
	m.refreshRecords.Inc()
}

// StateRefreshSkipped counts one refresh append skipped because a tombstone or
// newer write superseded the snapshot. That is a completion, not a failure.
func (m *Metrics) StateRefreshSkipped() {
	if m == nil {
		return
	}
	m.refreshSkipped.Inc()
}

// StateRefreshFailed counts one refresh append that failed and stays pending.
// It never moves colca_rejected_publishes_total, which is for client publishes.
func (m *Metrics) StateRefreshFailed() {
	if m == nil {
		return
	}
	m.refreshFailures.Inc()
}

// GapServed counts one gap served to a consumer on surface "fetch" or
// "downlink".
func (m *Metrics) GapServed(stream, surface string) {
	if m == nil {
		return
	}
	if bySurface, ok := m.gapServedBy[stream]; ok {
		if c, ok := bySurface[surface]; ok {
			c.Inc()
			return
		}
	}
	m.gapServed.WithLabelValues(stream, surface).Inc()
}

// GapReceived counts one gap a repl client saw on stream: an uplink low-water
// jump or a gap object from the parent.
func (m *Metrics) GapReceived(stream string) {
	if m == nil {
		return
	}
	if c, ok := m.gapReceivedBy[stream]; ok {
		c.Inc()
		return
	}
	m.gapReceived.WithLabelValues(stream).Inc()
}

// GapApplied counts one child-offset jump in IngestReplicated: records the
// child pruned before replicating them. Children are dynamic, so there is no
// pre-created child to hit.
func (m *Metrics) GapApplied(child, stream string) {
	if m == nil {
		return
	}
	m.replGapApplied.WithLabelValues(child, stream).Inc()
	m.replIntegrityFailures.Inc()
}

// DrainStarted counts one child node that started a move-drain.
func (m *Metrics) DrainStarted() {
	if m == nil {
		return
	}
	m.drainsActive.Inc()
}

// DrainPending sets the number of live, undelivered commands blocking child's
// drain. It is called on every completion check.
func (m *Metrics) DrainPending(child string, n int) {
	if m == nil {
		return
	}
	m.drainPendingCommands.WithLabelValues(child).Set(float64(n))
}

// DrainCompleted records a drain's outcome (one of DrainOutcome*) and drops
// child's pending-commands series, since the child has been revoked.
func (m *Metrics) DrainCompleted(child, outcome string) {
	if m == nil {
		return
	}
	m.drainsActive.Dec()
	m.drainPendingCommands.DeleteLabelValues(child)
	if c, ok := m.drainsCompletedBy[outcome]; ok {
		c.Inc()
		return
	}
	m.drainsCompleted.WithLabelValues(outcome).Inc()
}

// DefinitionApplied counts one definition applied from the downlink.
func (m *Metrics) DefinitionApplied() {
	if m == nil {
		return
	}
	m.definitionsApplied.Inc()
}

// DefinitionRejected counts one definition this node refused. Worth an alert:
// a rejected definition parks the definition cursor, so nothing behind it
// arrives either.
func (m *Metrics) DefinitionRejected() {
	if m == nil {
		return
	}
	m.definitionsRejected.Inc()
}

// BlobTransfer counts one completed blob transfer.
func (m *Metrics) BlobTransfer(direction, result string) {
	if m == nil {
		return
	}
	if c, ok := m.blobTransfersBy[direction+"|"+result]; ok {
		c.Inc()
		return
	}
	m.blobTransfers.WithLabelValues(direction, result).Inc()
}

// BlobRejected counts one blob refused at ingress.
func (m *Metrics) BlobRejected(reason string) {
	if m == nil {
		return
	}
	if c, ok := m.blobRejectsBy[reason]; ok {
		c.Inc()
		return
	}
	m.blobRejects.WithLabelValues(reason).Inc()
}

// RecordRejected counts one record refused for exceeding the size cap.
func (m *Metrics) RecordRejected(reason string) {
	if m == nil {
		return
	}
	if c, ok := m.recordRejectsBy[reason]; ok {
		c.Inc()
		return
	}
	m.recordRejects.WithLabelValues(reason).Inc()
}

// HTTPRequestLimited counts one 429 response. Callers supply only fixed door
// and route-class constants, so the metric cannot acquire attacker-controlled
// label cardinality.
func (m *Metrics) HTTPRequestLimited(door, class string) {
	if m != nil {
		m.httpRequestLimited.WithLabelValues(door, class).Inc()
	}
}

// HTTPKVRead counts one GET /kv page and the entries it returned. contract is
// one contract name, "multiple" or "all"; prefixDepth is already bounded.
func (m *Metrics) HTTPKVRead(caller, contract, prefixDepth string, entries int) {
	if m != nil {
		m.httpKVRequests.WithLabelValues(caller, contract, prefixDepth).Inc()
		m.httpKVEntries.WithLabelValues(caller).Observe(float64(entries))
	}
}

// HTTPFetch counts one GET /fetch page. stream is a known stream.
func (m *Metrics) HTTPFetch(caller, stream string) {
	if m != nil {
		m.httpFetchRequests.WithLabelValues(caller, stream).Inc()
	}
}

// HTTPLimitedCaller counts one 429 by route pattern (the mux pattern, a fixed
// set) and caller.
func (m *Metrics) HTTPLimitedCaller(route, caller string) {
	if m != nil {
		m.httpLimitedByCaller.WithLabelValues(route, caller).Inc()
	}
}

// HTTPCallerSeen creates the caller's limited series for route at 0 the first
// time the pair is admitted. It has the cardinality the counter reaches anyway
// once that caller is limited on that route.
func (m *Metrics) HTTPCallerSeen(route, caller string) {
	if m == nil {
		return
	}
	key := route + "\x00" + caller
	if _, seen := m.httpCallersSeen.Load(key); seen {
		return
	}
	m.httpLimitedByCaller.WithLabelValues(route, caller)
	m.httpCallersSeen.Store(key, struct{}{})
}

// ResourceRead counts one read of GET /resources/{id}/file, by result.
func (m *Metrics) ResourceRead(result string) {
	if m == nil {
		return
	}
	if c, ok := m.resourceReadsBy[result]; ok {
		c.Inc()
		return
	}
	m.resourceReads.WithLabelValues(result).Inc()
}

// BlobSwept counts one unreferenced blob the sweeper deleted after its grace
// period.
func (m *Metrics) BlobSwept() {
	if m == nil {
		return
	}
	m.blobsSwept.Inc()
}

// MetricUnbound counts one _Metric accepted on a path with no _Signal.
func (m *Metrics) MetricUnbound() {
	if m == nil {
		return
	}
	m.metricsUnbound.Inc()
}

// storeCollector derives the gauge families from the store (and, for the
// retention families, the node's retention policy) at scrape time.
type storeCollector struct {
	st  *store.Store
	cfg config.Retention
	// scanCap bounds the policy target scan in blockedByCursor. Tests pass a small
	// value.
	scanCap uint64
}

// newStoreCollector builds the collector; tests use it to pass a small scan cap.
func newStoreCollector(st *store.Store, cfg config.Retention, scanCap uint64) *storeCollector {
	return &storeCollector{st: st, cfg: cfg, scanCap: scanCap}
}

var (
	descNextOffset = prometheus.NewDesc("colca_stream_next_offset",
		"Next offset the stream will assign (derived from the store at scrape time).",
		[]string{"stream"}, nil)
	descLWM = prometheus.NewDesc("colca_stream_low_water_mark",
		"Lowest offset still retained (derived from the store at scrape time); next_offset - low_water_mark = live records.",
		[]string{"stream"}, nil)
	descLiveBytes = prometheus.NewDesc("colca_stream_live_bytes",
		"Live logical bytes retained on the stream (derived from the store at scrape time).",
		[]string{"stream"}, nil)
	descCursorPos = prometheus.NewDesc("colca_cursor_position",
		"Next offset the named cursor will read (derived from the store at scrape time).",
		[]string{"cursor", "stream"}, nil)
	descCursorLag = prometheus.NewDesc("colca_cursor_lag_records",
		"Records the cursor has not read yet: next_offset - position, floored at 0.",
		[]string{"cursor", "stream"}, nil)
	descCursorAdvanceAge = prometheus.NewDesc("colca_cursor_last_advance_age_seconds",
		"Seconds since the cursor last advanced, which is what staleness is measured from; 0 for a cursor never seen advancing.",
		[]string{"cursor", "stream"}, nil)
	descCursorNextRecordAge = prometheus.NewDesc("colca_cursor_next_record_age_seconds",
		"Age of the next retained record awaiting this cursor, floored at zero; zero when caught up. Includes local-only records awaiting uplink progress, not only uploadable samples.",
		[]string{"cursor", "stream"}, nil)
	descRetentionPressure = prometheus.NewDesc("colca_retention_pressure",
		"max(age_used/max_age, live_bytes/max_bytes) for the stream's currently retained window; >1 means policy wants to prune further but is cursor-clamped.",
		[]string{"stream"}, nil)
	descBlockedByCursor = prometheus.NewDesc("colca_retention_blocked_by_cursor",
		"Count of cursors currently clamping the stream below where the age/size policy would otherwise prune to. "+
			"The policy target itself is scanned under a bounded record cap (store.DefaultPolicyScanCap): in an "+
			"extreme backlog (e.g. a long-dead consumer) the scan may stop before reaching the policy's true target, "+
			"in which case this gauge counts cursors below that capped FLOOR instead — every counted cursor is still "+
			"exactly blocked, so the value can only undercount, never overcount. colca_stream_live_bytes is the "+
			"O(1), never-capped signal for unbounded backlog pressure.",
		[]string{"stream"}, nil)
	descChildHWM = prometheus.NewDesc("colca_child_hwm",
		"Highest child offset already applied, per (child, stream).",
		[]string{"child", "stream"}, nil)
)

func (c *storeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descNextOffset
	ch <- descLWM
	ch <- descLiveBytes
	ch <- descCursorPos
	ch <- descCursorLag
	ch <- descCursorAdvanceAge
	ch <- descCursorNextRecordAge
	ch <- descRetentionPressure
	ch <- descBlockedByCursor
	ch <- descChildHWM
}

func (c *storeCollector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now()
	for _, s := range streams {
		ch <- prometheus.MustNewConstMetric(descNextOffset, prometheus.GaugeValue,
			float64(c.st.NextOffset(s)), s)
		ch <- prometheus.MustNewConstMetric(descLWM, prometheus.GaugeValue,
			float64(c.st.LWM(s)), s)
		ch <- prometheus.MustNewConstMetric(descLiveBytes, prometheus.GaugeValue,
			float64(c.st.StreamBytes(s)), s)
	}
	for _, cur := range c.st.Cursors() {
		ch <- prometheus.MustNewConstMetric(descCursorPos, prometheus.GaugeValue,
			float64(cur.Position), cur.Name, cur.Stream)
		var lag uint64
		if next := c.st.NextOffset(cur.Stream); next > cur.Position {
			lag = next - cur.Position
		}
		ch <- prometheus.MustNewConstMetric(descCursorLag, prometheus.GaugeValue,
			float64(lag), cur.Name, cur.Stream)
		var age float64
		if cur.LastAdvanceMS > 0 {
			age = now.Sub(time.UnixMilli(cur.LastAdvanceMS)).Seconds()
		}
		ch <- prometheus.MustNewConstMetric(descCursorAdvanceAge, prometheus.GaugeValue,
			age, cur.Name, cur.Stream)
		// A point lookup stays bounded even when a node has months of backlog.
		// Cursor inactivity and queued-record age are different observations.
		pos := max(cur.Position, c.st.LWM(cur.Stream))
		var recordAge float64
		if pos < c.st.NextOffset(cur.Stream) {
			err := c.st.ScanRecords(cur.Stream, pos, pos+1, func(_ uint64, ts int64, _ uint64) bool {
				recordAge = math.Max(0, now.Sub(time.UnixMilli(ts)).Seconds())
				return false
			})
			if err != nil {
				ch <- prometheus.NewInvalidMetric(descCursorNextRecordAge, err)
				continue
			}
		}
		ch <- prometheus.MustNewConstMetric(descCursorNextRecordAge, prometheus.GaugeValue,
			recordAge, cur.Name, cur.Stream)
	}
	for _, s := range retentionStreams {
		ch <- prometheus.MustNewConstMetric(descRetentionPressure, prometheus.GaugeValue,
			c.pressure(s, now), s)
		ch <- prometheus.MustNewConstMetric(descBlockedByCursor, prometheus.GaugeValue,
			float64(c.blockedByCursor(s, now)), s)
	}
	for _, h := range c.st.HWMs() {
		ch <- prometheus.MustNewConstMetric(descChildHWM, prometheus.GaugeValue,
			float64(h.HWM), h.Child, h.Stream)
	}
}

// pressure is max(age_used/max_age, live_bytes/max_bytes), where age_used is
// the age of the oldest retained record. A term is left out when its limit is
// unset.
func (c *storeCollector) pressure(stream string, now time.Time) float64 {
	pol := c.cfg.EffectiveStream(stream)
	maxAge := time.Duration(pol.MaxAge)
	maxBytes := uint64(pol.MaxBytes)
	var p float64
	if maxAge > 0 {
		if ts, ok := c.oldestTS(stream); ok {
			if r := now.Sub(time.UnixMilli(ts)).Seconds() / maxAge.Seconds(); r > p {
				p = r
			}
		}
	}
	if maxBytes > 0 {
		if r := float64(c.st.StreamBytes(stream)) / float64(maxBytes); r > p {
			p = r
		}
	}
	return p
}

// oldestTS returns the timestamp of the oldest retained record; ok is false
// when the stream is empty. Errors are ignored, as this only feeds a gauge.
func (c *storeCollector) oldestTS(stream string) (ts int64, ok bool) {
	lwm := c.st.LWM(stream)
	_ = c.st.ScanRecords(stream, lwm, lwm+1, func(_ uint64, t int64, _ uint64) bool {
		ts, ok = t, true
		return false
	})
	return ts, ok
}

// blockedByCursor counts the protected cursors below the offset the age and
// size policy would prune to, using the pruner's own store functions,
// read-only. The scan is capped because this gauge matters most when a dead
// consumer lets the backlog grow; past the cap the target is a floor, so the
// count can undercount but never overcount.
func (c *storeCollector) blockedByCursor(stream string, now time.Time) int {
	pol := c.cfg.EffectiveStream(stream)
	maxAge := time.Duration(pol.MaxAge)
	maxBytes := uint64(pol.MaxBytes)
	window := time.Duration(pol.IgnoreCursorsAfter)
	if maxAge <= 0 && maxBytes == 0 {
		return 0 // no policy configured: nothing can ever be "blocked"
	}
	lwm := c.st.LWM(stream)
	next := c.st.NextOffset(stream)
	liveBytes := c.st.StreamBytes(stream)

	target, _, _, _ := c.st.PolicyPruneTarget(stream, lwm, next, now, maxAge, maxBytes, liveBytes, next, c.scanCap)
	if target <= lwm {
		return 0 // policy wants nothing further: no cursor can be "blocking"
	}

	protecting, _ := c.st.ProtectedCursors(stream, now, window)
	blocked := 0
	for _, cur := range protecting {
		if cur.Position < target {
			blocked++
		}
	}
	return blocked
}
