// Package metrics owns Colca's Prometheus surface: cheap
// atomic counters incremented from the hot paths, and a store-reading
// collector that derives every gauge (stream offsets, cursor lag, child HWMs,
// low-water marks, retention pressure) from the store AT SCRAPE TIME — no
// background sampling, no self-reported state.
//
// The family names and labels below are a contract: the retention plan
// (the retention design §8) references
// them.
//
// Every increment method is safe on a nil *Metrics receiver (no-op), so the
// engine, broker, repl and retention packages stay testable without a
// registry.
package metrics

import (
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Reject reasons — the allowed label values of colca_rejected_publishes_total.
const (
	ReasonNodeID      = "node_id"      // topic level 4 is not this node
	ReasonGrammar     = "grammar"      // topic does not parse as uns grammar
	ReasonValidation  = "validation"   // payload fails the contract's schema
	ReasonIdentity    = "identity"     // payload authorship contradicts the authenticated identity
	ReasonWriteDenied = "write_denied" // no write scope covers the topic
	ReasonCmdDenied   = "cmd_denied"   // a client's _Cmd* publish had no covering cmd grant
	// ReasonRegistryContract: _EnrolledIdentity arrived at an ordinary ingest door —
	// registry entries enter only through the enrollment endpoint (auth §3).
	ReasonRegistryContract = "registry_contract"
	// ReasonHumanWrite: a human published a data/entity/ack contract — the
	// World-2 rule (humans command, machines write state; human-authz §5.2).
	ReasonHumanWrite = "human_write"
	// ReasonTimeSync: a _TimeSync publish arrived from a client, an admin
	// caller, or a replicated batch (time-sync design §2.2/§4). _TimeSync is
	// ephemeral and node-local-publish-only — only the node's own beacon loop
	// may ever produce it, straight to the local bus, never through an
	// ingest door.
	ReasonTimeSync = "time_sync"
	// ReasonDraining: a ClassCmd publish (client or admin) targeted a mount
	// currently under move-drain (move-drain design §3.2 item 2/§3.4) — new
	// commands are refused at admission so the drain converges instead of
	// chasing a moving tail.
	ReasonDraining = "draining"
)

var reasons = []string{ReasonNodeID, ReasonGrammar, ReasonValidation, ReasonIdentity, ReasonWriteDenied, ReasonCmdDenied, ReasonRegistryContract, ReasonHumanWrite, ReasonTimeSync, ReasonDraining}

// Move-drain outcome labels — the allowed `outcome` values of
// colca_drains_completed_total (move-drain design §3.2/§3.4).
const (
	DrainOutcomeDelivered = "delivered" // the commands queue was already empty at completion
	DrainOutcomeExpired   = "expired"   // undelivered leftovers timed out (expires_at < now)
	DrainOutcomeForced    = "forced"    // DELETE /enroll/{ulid} interrupted an active drain
	// DrainOutcomeGapped [delta]: retention pruned some or all of a
	// draining child's undelivered commands before they were fetched or
	// expired — a distinct, honest outcome so
	// "delivered" never silently covers for data destroyed by an unrelated
	// retention policy. Not in the original design §3.4 list; documented
	// there with a [delta] marker alongside this constant.
	DrainOutcomeGapped = "gapped"
)

var drainOutcomes = []string{DrainOutcomeDelivered, DrainOutcomeExpired, DrainOutcomeForced, DrainOutcomeGapped}

// Auth doors and rejection reasons — the label values of
// colca_auth_rejections_total{door,reason} (auth design §9). CONNECT/request
// authentication failures live here, NOT in colca_rejected_publishes_total:
// that family counts publishes, this one counts identities turned away at a
// door.
const (
	DoorMQTT = "mqtt"
	DoorHTTP = "http"
	DoorRepl = "repl"
	// DoorLocal is the unpublished, plaintext local door (local-service-trust
	// design §4): reachability is the credential, so rejections here are rare
	// (a missing name, or a name colliding with a keyed identity's ULID) but
	// still counted like every other door.
	DoorLocal = "local"

	AuthUnknownKey       = "unknown_key"       // TLS peer key not in the local registry (incl. revoked)
	AuthKind             = "kind"              // entry exists but its kind may not use this door
	AuthUsernameMismatch = "username_mismatch" // MQTT username != the key's enrolled ULID
	AuthToken            = "token"             // admin token missing or wrong
	AuthNoName           = "no_name"           // local door CONNECT carried no username
	AuthRegister         = "register"          // local door self-registration failed
)

var authDoors = []string{DoorMQTT, DoorHTTP, DoorRepl, DoorLocal}
var authReasons = []string{AuthUnknownKey, AuthKind, AuthUsernameMismatch, AuthToken, AuthNoName, AuthRegister}

// ACL denial actions — label values of colca_acl_denials_total{action}:
// read-side denials only (sub = MQTT subscribe filter, read = HTTP record
// scope). Write-side rejections stay in colca_rejected_publishes_total.
const (
	ACLSub  = "sub"
	ACLRead = "read"
)

var aclActions = []string{ACLSub, ACLRead}

// streams is every persistent stream — what has an offset, live bytes and an
// ingest count. DERIVED from the store, never mirrored: a copy here could not
// tell that the store's set grew, and the families below would then silently
// cover one stream fewer than they claim to.
var streams = store.Streams()

// uplinkStreams is the subset that RISES. `definitions` is absent because they
// descend and never rise (definition-stream design §4): a "last uplink success"
// gauge for a stream the uplink never touches would sit at zero forever and read
// exactly like a broken uplink.
var uplinkStreams = []string{"metrics", "entities", "commands", "audit", "alarms"}

// retentionStreams is the subset the retention POLICY applies to. `definitions`
// is absent for the same reason it is absent from the pruner's own list
// (definition-stream design §6): it is never pruned by age or size, so a
// pressure or blocked-by-cursor gauge for it would report progress toward a
// policy that does not exist.
var retentionStreams = []string{"metrics", "entities", "commands", "audit", "alarms"}

// gapSurfaces — the allowed `surface` label values of colca_gap_served_total
// (design §8): `fetch` is GET /fetch (any stream), `downlink` is GET
// /downlink (commands only, in practice — pre-created for every stream
// anyway so the family never has to grow a child mid-flight).
var gapSurfaces = []string{"fetch", "downlink"}

// Metrics is the node's metric registry plus the pre-created children the hot
// paths increment. Labels are resolved once at construction — never per
// message.
type Metrics struct {
	reg *prometheus.Registry

	ingest       *prometheus.CounterVec
	rejected     *prometheus.CounterVec
	uplinkOK     *prometheus.GaugeVec
	uplinkFail   *prometheus.CounterVec
	downlinkOK   prometheus.Gauge
	downlinkFail prometheus.Counter
	reseed       prometheus.Gauge
	authReject   *prometheus.CounterVec
	aclDeny      *prometheus.CounterVec
	kicks        prometheus.Counter
	// Human world (human-authz design §7).
	humanSessions prometheus.Gauge   // colca_human_sessions
	jwksKeys      prometheus.Gauge   // colca_jwks_keys
	jwksFailures  prometheus.Counter // colca_jwks_refresh_failures_total

	// CmdAdmin world (cmdadmin design §9).
	nodeCmds   *prometheus.CounterVec // colca_node_cmds_total{contract,verb,result}
	nodePrefix *prometheus.GaugeVec   // colca_node_prefix_info{prefix}

	// Schema bundle (schema-bundle design §11).
	bundleInfo      *prometheus.GaugeVec // colca_contracts_bundle_info{version,digest,source}
	bundleContracts prometheus.Gauge     // colca_contracts_bundle_contracts

	// Retention (design §8): pruner-side counters.
	prunedRecords *prometheus.CounterVec // colca_retention_pruned_records_total{stream}
	prunedBytes   *prometheus.CounterVec // colca_retention_pruned_bytes_total{stream}
	pruneRuns     *prometheus.CounterVec // colca_retention_prune_runs_total{stream}
	gapRecords    *prometheus.CounterVec // colca_retention_gap_records_total{stream}
	// State refresh (design §6.5/§8): unlabeled — only the entities stream
	// ever refreshes. refreshFailures is the counter the pruner's §6.5 append
	// failures move now, replacing the RejectPublish mislabel IngestRefresh
	// used to fall through to (a refresh append is an internal repair action,
	// never a client's rejected publish).
	refreshRecords  prometheus.Counter // colca_retention_state_refresh_records_total
	refreshSkipped  prometheus.Counter // colca_retention_state_refresh_skipped_total
	refreshFailures prometheus.Counter // colca_retention_state_refresh_failures_total

	// Gap contract (design §6/§8).
	gapServed      *prometheus.CounterVec // colca_gap_served_total{stream,surface}
	gapReceived    *prometheus.CounterVec // colca_gap_received_total{stream}
	replGapApplied *prometheus.CounterVec // colca_repl_gap_applied_total{child,stream}

	// Move-drain (design §3.2/§3.4).
	drainsActive         prometheus.Gauge       // colca_drains_active
	drainPendingCommands *prometheus.GaugeVec   // colca_drain_pending_commands{child}
	drainsCompleted      *prometheus.CounterVec // colca_drains_completed_total{outcome}
	definitionsApplied   prometheus.Counter     // colca_definitions_applied_total
	definitionsRejected  prometheus.Counter     // colca_definitions_rejected_total
	auditWriteFailures   prometheus.Counter     // colca_audit_write_failures_total
	drainsCompletedBy    map[string]prometheus.Counter

	ingestBy        map[string]prometheus.Counter
	rejectedBy      map[string]prometheus.Counter
	uplinkOKBy      map[string]prometheus.Gauge
	uplinkFailBy    map[string]prometheus.Counter
	aclDenyBy       map[string]prometheus.Counter
	prunedRecordsBy map[string]prometheus.Counter
	prunedBytesBy   map[string]prometheus.Counter
	pruneRunsBy     map[string]prometheus.Counter
	gapRecordsBy    map[string]prometheus.Counter
	gapServedBy     map[string]map[string]prometheus.Counter // [stream][surface]
	gapReceivedBy   map[string]prometheus.Counter
}

// New builds the registry: the store collector plus every counter/gauge
// family, pre-created and zero-valued so all families are present from the
// first scrape. cfg is the node's retention policy — the collector needs it
// to derive colca_retention_pressure and colca_retention_blocked_by_cursor at
// scrape time (EffectiveStream applies the §3.1 defaults the same way the
// pruner does).
//
// clk is the node's authoritative-time state (time-sync design §2.1/§2.4);
// it must be the SAME *clock.Clock instance passed to the node's
// *engine.Engine (see engine.New's doc comment) or these gauges report state
// nobody ever updates. clk may be nil (unit tests that do not exercise
// time-sync): colca_clock_offset_ms reads 0 and colca_clock_sync_age_seconds
// reads +Inf ("never synced"), same as a non-root node before its first
// sample.
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
		downlinkOK: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_downlink_last_success_timestamp_seconds",
			Help: "Unix time of the last successful downlink fetch, empty fetches included (0 = never this process).",
		}),
		downlinkFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_downlink_fetch_failures_total",
			Help: "Failed downlink fetches. Resets on restart.",
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
		nodePrefix: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "colca_node_prefix_info",
			Help: "The node's root-frame prefix as taught by its parent (info gauge, value 1; absent until learned).",
		}, []string{"prefix"}),
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
			Help: "Durable _StreamGap records emitted by the pruner (design §6.4), by stream. Resets on restart.",
		}, []string{"stream"}),
		refreshRecords: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_retention_state_refresh_records_total",
			Help: "KV entries re-appended to entities by the §6.5 state refresh. Resets on restart.",
		}),
		refreshSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_retention_state_refresh_skipped_total",
			Help: "State-refresh appends skipped because the snapshot was superseded (tombstone or newer write, spec §6.5/§7.1). Resets on restart.",
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
			Help: "Child-offset jumps observed in ApplyReplicated (design §6.4 second net), by child and stream. Resets on restart.",
		}, []string{"child", "stream"}),
		drainsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_drains_active",
			Help: "Enrolled kind=node children currently in a move-drain decommission (move-drain design §3.1/§3.4).",
		}),
		drainPendingCommands: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "colca_drain_pending_commands",
			Help: "Live, undelivered ClassCmd records still blocking a draining child's completion, by child (move-drain design §3.2 item 3/§3.4).",
		}, []string{"child"}),
		drainsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_drains_completed_total",
			Help: "Move-drains that reached a terminal outcome, by outcome: delivered (queue empty), expired (leftovers timed out), forced (DELETE during drain). Resets on restart.",
		}, []string{"outcome"}),
		definitionsApplied: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_definitions_applied_total",
			Help: "Definitions handed down by the parent and applied here as state (definition-stream design §5). Resets on restart; the current SET is the KV view, not this counter.",
		}),
		definitionsRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_definitions_rejected_total",
			Help: "Definitions the parent handed down that this node refused (bad grammar, wrong class, failed validation). Non-zero means policy or types are NOT arriving and the node's cursor is parked on the offending record — always worth an alert.",
		}),
		auditWriteFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_audit_write_failures_total",
			Help: "Security audit events that could not be durably appended. The protected operation remains denied. Resets on restart.",
		}),
	}
	m.ingestBy = counterChildren(m.ingest, streams)
	m.rejectedBy = counterChildren(m.rejected, reasons)
	m.uplinkFailBy = counterChildren(m.uplinkFail, uplinkStreams)
	m.aclDenyBy = counterChildren(m.aclDeny, aclActions)
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

	// colca_drains_active starts at the count of entries persisted with
	// status=draining (move-drain design §3.2: "status survives restart") —
	// runtime Inc/Dec (DrainStarted/DrainCompleted) only ever adjusts THIS
	// process' counter, so re-deriving the starting point from the store on
	// every construction is what keeps it correct across a restart instead
	// of silently resetting to 0 while children are still mid-drain.
	draining := 0
	for _, raw := range st.RegistryScan() {
		// Decoded as the real entry rather than a local struct carrying just
		// the status field: the entry's JSON shape has one owner, and asking
		// it whether it drains keeps that rule out of here entirely.
		var e uns.Entry
		if err := json.Unmarshal(raw, &e); err == nil && e.IsDraining() {
			draining++
		}
	}
	m.drainsActive.Set(float64(draining))

	// Time-sync (design §2.1/§2.4): read directly from clk at scrape time —
	// GaugeFunc, not a pushed Set(), because colca_clock_sync_age_seconds is
	// genuinely "seconds since the last sample right now", not a value any
	// write path could push in advance. clk == nil (unit tests that never
	// wire time-sync) reads exactly like a non-root node that has never
	// synced: offset 0, age +Inf.
	clockOffset := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "colca_clock_offset_ms",
		Help: "Current authoritative-time offset estimate in milliseconds (design §2.1/§2.4): offset_ms = now_ms - wall_receipt from the most recent /downlink or /replicate response. Always 0 on the root and on a node that has never synced.",
	}, func() float64 {
		if clk == nil {
			return 0
		}
		return float64(clk.OffsetMS())
	})
	clockSyncAge := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "colca_clock_sync_age_seconds",
		Help: "Seconds since the last accepted offset sample (design §2.4). The root exports 0 by definition. +Inf means never synced.",
	}, func() float64 {
		if clk == nil {
			return math.Inf(1)
		}
		// clk.Now(), not time.Now(): this stays on the SAME injectable clock
		// the rest of the time-sync decision logic uses (mandatory per
		// design §2.1 — no bare time.Now() in this feature's decision code),
		// so a test with a fake clock sees a deterministic age.
		return clk.SyncAgeSeconds(clk.Now())
	})

	m.reg.MustRegister(m.ingest, m.rejected, m.uplinkOK, m.uplinkFail,
		m.downlinkOK, m.downlinkFail, m.reseed,
		m.authReject, m.aclDeny, m.kicks, m.humanSessions, m.jwksKeys, m.jwksFailures,
		m.nodeCmds, m.nodePrefix, m.bundleInfo, m.bundleContracts,
		m.prunedRecords, m.prunedBytes, m.pruneRuns, m.gapRecords,
		m.refreshRecords, m.refreshSkipped, m.refreshFailures,
		m.gapServed, m.gapReceived, m.replGapApplied,
		m.drainsActive, m.drainPendingCommands, m.drainsCompleted,
		m.definitionsApplied, m.definitionsRejected, m.auditWriteFailures,
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

// SessionKick counts one live session disconnected by a registry change.
// SetHumanSessions tracks the live human-door session count.
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
// outcome (ok | conflict | invalid | expired | error).
func (m *Metrics) NodeCmd(contract, verb, result string) {
	if m == nil {
		return
	}
	m.nodeCmds.WithLabelValues(contract, verb, result).Inc()
}

// SetBundleInfo reports the active contract authority (schema-bundle design
// §11): version/digest of the loaded bundle, or source=builtin for the floor.
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

func (m *Metrics) SessionKick() {
	if m == nil {
		return
	}
	m.kicks.Inc()
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

// DownlinkFetched records a successful downlink fetch — empty fetches count:
// progress means the loop is alive, not that data flowed.
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

// SetReseedCount records how many KV entries the startup reseed replayed.
func (m *Metrics) SetReseedCount(n int) {
	if m == nil {
		return
	}
	m.reseed.Set(float64(n))
}

// RetentionPruneRun counts one prune cycle that actually removed at least one
// record from stream (design §8, colca_retention_prune_runs_total) — a cycle
// that evaluated the policy and found nothing to do does not count.
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

// RetentionPruned adds one run's removed records and shed logical bytes to
// stream's running totals (design §8, colca_retention_pruned_records_total /
// colca_retention_pruned_bytes_total). Both records and bytes must be the
// store's own authoritative post-commit figures (store.Prune's return value
// and store.PruneSpan.Shed respectively) — the store's in-batch cursor
// recheck (spec §5.2 [delta]) can shrink the pruned range after the caller's
// own pre-commit scan, and only the store's own accounting reflects what was
// actually removed.
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

// RetentionGapRecorded counts one durable _StreamGap record emitted for
// stream (design §6.4/§8, colca_retention_gap_records_total).
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

// StateRefreshApplied counts one entities KV entry the §6.5 state refresh
// successfully re-appended (design §8, colca_retention_state_refresh_records_total).
func (m *Metrics) StateRefreshApplied() {
	if m == nil {
		return
	}
	m.refreshRecords.Inc()
}

// StateRefreshSkipped counts one state-refresh append the CAS guard skipped
// because the snapshot was superseded by a tombstone or a newer write (spec
// §6.5/§7.1) — a completion, not a failure.
func (m *Metrics) StateRefreshSkipped() {
	if m == nil {
		return
	}
	m.refreshSkipped.Inc()
}

// StateRefreshFailed counts one state-refresh append that errored and left
// the refresh obligation pending for retry. This is the refresh path's OWN
// failure counter — refresh appends must never move
// colca_rejected_publishes_total, which describes rejected client/admin
// publishes, not the pruner's internal repair traffic.
func (m *Metrics) StateRefreshFailed() {
	if m == nil {
		return
	}
	m.refreshFailures.Inc()
}

// GapServed counts one wire gap served to a consumer (design §6/§8,
// colca_gap_served_total). surface is "fetch" (GET /fetch) or "downlink"
// (GET /downlink).
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

// GapReceived counts one gap a repl client observed on stream: an uplink LWM
// jump (the local pruner overrode the uplink cursor) or a downlink gap object
// from the parent (design §6.3/§8, colca_gap_received_total).
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

// GapApplied counts one child-offset jump observed in IngestReplicated —
// records the parent never received because the child pruned past them
// before replicating (design §6.4 second net/§8, colca_repl_gap_applied_total).
// child and stream are dynamic (children are configured per node), so unlike
// the fixed-label counters above there is no pre-created child to hit.
func (m *Metrics) GapApplied(child, stream string) {
	if m == nil {
		return
	}
	m.replGapApplied.WithLabelValues(child, stream).Inc()
}

// DrainStarted increments colca_drains_active — one enrolled kind=node child
// began a move-drain decommission (move-drain design §3.1, POST
// /enroll/{ulid}/drain).
func (m *Metrics) DrainStarted() {
	if m == nil {
		return
	}
	m.drainsActive.Inc()
}

// DrainPending sets colca_drain_pending_commands{child} to the count of
// live, undelivered ClassCmd records currently blocking child's drain
// (move-drain design §3.2 item 3/§3.4). Called on every completion-predicate
// evaluation, whether or not the drain completes this round.
func (m *Metrics) DrainPending(child string, n int) {
	if m == nil {
		return
	}
	m.drainPendingCommands.WithLabelValues(child).Set(float64(n))
}

// DrainCompleted records one move-drain's terminal outcome (move-drain
// design §3.4, one of DrainOutcome*): decrements colca_drains_active and
// clears child's now-meaningless colca_drain_pending_commands series — by
// the time a caller reaches this method, the registry has already revoked
// child (or is in the process of the same DELETE that produced a "forced"
// outcome), so the series describes an identity that no longer exists.
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

// DefinitionApplied counts one definition applied from the downlink
// (definition-stream design §5).
func (m *Metrics) DefinitionApplied() {
	if m == nil {
		return
	}
	m.definitionsApplied.Inc()
}

// DefinitionRejected counts one definition this node refused to apply.
//
// Worth alerting on: unlike a rejected client publish, which costs one client
// one message, a rejected definition parks the node's definition cursor — so
// nothing behind it arrives either, and the node quietly stops learning about
// new groups and types.
func (m *Metrics) DefinitionRejected() {
	if m == nil {
		return
	}
	m.definitionsRejected.Inc()
}

// storeCollector derives the gauge families from the store (and, for the
// retention families, the node's retention policy) at scrape time.
type storeCollector struct {
	st  *store.Store
	cfg config.Retention
	// scanCap bounds blockedByCursor's PolicyPruneTarget scan (see
	// store.DefaultPolicyScanCap). Always the production default via New;
	// tests construct a collector directly with a small value to prove the
	// walk is bounded without seeding hundreds of thousands of records.
	scanCap uint64
}

// newStoreCollector builds the collector with an explicit scan cap — New
// (the production constructor) always passes store.DefaultPolicyScanCap;
// this seam exists so tests can pass a small cap instead.
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
		"Seconds since the cursor last advanced (staleness input of design §5.2); 0 for a cursor never seen advancing.",
		[]string{"cursor", "stream"}, nil)
	descRetentionPressure = prometheus.NewDesc("colca_retention_pressure",
		"max(age_used/max_age, live_bytes/max_bytes) for the stream's currently retained window; >1 means policy wants to prune further but is cursor-clamped (design §5.2).",
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

// pressure computes design §8's colca_retention_pressure for stream:
// max(age_used/max_age, live_bytes/max_bytes), where age_used is the age of
// the OLDEST currently retained record (the one sitting at the LWM) — not a
// re-run of the pruner's scan. A term is omitted when its limit is unset
// (age_used/max_age when max_age<=0, live_bytes/max_bytes when max_bytes==0),
// matching EffectiveStream's "either limit is sufficient to enable pruning"
// contract (config.go).
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

// oldestTS returns the timestamp of the record at stream's current LWM — the
// oldest record still retained — via a single-record ScanRecords window. ok
// is false when the stream currently retains nothing (LWM == next_offset). A
// scan error is swallowed (same convention as KVScan): this is a best-effort
// scrape-time gauge, not a correctness path.
func (c *storeCollector) oldestTS(stream string) (ts int64, ok bool) {
	lwm := c.st.LWM(stream)
	_ = c.st.ScanRecords(stream, lwm, lwm+1, func(_ uint64, t int64, _ uint64) bool {
		ts, ok = t, true
		return false
	})
	return ts, ok
}

// blockedByCursor derives design §8's colca_retention_blocked_by_cursor for
// stream: the count of protected cursors sitting below the offset the
// age/size policy would prune to if no cursor existed. Built entirely from
// store.ProtectedCursors and store.PolicyPruneTarget — the exact same
// classification and scan retention.Pruner.pruneStream uses for the real
// clamp/override decision (drift between the two is structurally
// impossible; there is only one implementation) — called here read-only
// (no CursorMarkSeen, no batch, no mutation) and unclamped (clamp=next: how
// far the policy would go with no cursor floor at all).
//
// The scan is bounded by c.scanCap (store.DefaultPolicyScanCap in
// production): in the exact alert state this gauge exists to catch — a dead
// consumer, default ignore_cursors_after=0 never overriding, backlog
// growing unbounded — an uncapped scan here would JSON-decode the entire
// clamped backlog on every single scrape. When the cap is hit, target is a
// FLOOR (see PolicyPruneTarget's doc): every cursor counted below it is
// still exactly blocked, so this can only undercount cursors sitting
// between the floor and the true (unscanned) target, never overcount.
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
