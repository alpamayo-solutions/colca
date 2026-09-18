// Generated from the contracts bundle — do not edit.
//
// make bundle && npm run generate:types

/** `_Ack` — stream class `ack`. */
export type Ack = {
  correlation_id: string;
  message?: string;
  performed_at?: null | number;
  result_code: number;
  [key: string]: unknown;
};

/** `_AlarmNotificationConfig` — stream class `entity`. */
export type AlarmNotificationConfig = {
  active_silences: Record<string, unknown>[];
  alarms: Record<string, unknown>[];
  channels: {
    enabled?: boolean;
    id: string;
    kind: string;
    name: string;
    public_config: Record<string, unknown>;
    rate_limit_per_minute?: number;
    sealed_secret: null | {
      algorithm: string;
      ciphertext: string;
      key_id: string;
      version: number;
    };
  }[];
  id: string;
  issued_at: number;
  policies: Record<string, unknown>[];
  policy_targets: Record<string, unknown>[];
  recipients: Record<string, unknown>[];
  revision_id: string;
  schema_version: number;
  target_node_id: string;
  [key: string]: unknown;
};

/** `_AlarmState` — stream class `entity`. */
export type AlarmState = {
  acknowledged_at?: null | number;
  acknowledged_by?: null | string;
  alarm_id: string;
  event_id?: null | string;
  note?: null | string;
  op?: null | string;
  reason: string;
  severity: "info" | "warning" | "critical";
  signal_id: string;
  silenced_by?: null | string;
  silenced_until?: null | number;
  since: number;
  status: "pending" | "firing" | "unknown";
  threshold?: null | number;
  value?: unknown;
  [key: string]: unknown;
};

/** `_AlarmStateChange` — stream class `alarm`. */
export type AlarmStateChange = {
  alarm_id: string;
  at: number;
  event_id: string;
  from_status: null | string;
  metadata_json?: Record<string, unknown>;
  notification: {
    channels?: {
      attempt?: number;
      channel_id: string;
      channel_kind: string;
      error_code?: null | string;
      policy_id?: null | string;
      provider_message_id?: null | string;
      recipient_id?: null | string;
      sent: boolean;
      status: string;
      target_id?: null | string;
    }[];
    requested: boolean;
    status: string;
  };
  notification_settings?: null | Record<string, unknown>;
  revision: number;
  signal_value: unknown;
  to_status: string;
  [key: string]: unknown;
};

/** `_Annotation` — stream class `annotation`. */
export type Annotation = {
  annotation_id: string;
  annotation_type_id: string;
  deleted?: boolean;
  revision?: number;
  signal_ids?: string[];
  source?: string;
  time_end?: null | number;
  time_start: number;
  value?: unknown;
  [key: string]: unknown;
};

/** `_AnnotationType` — stream class `definition`. */
export type AnnotationType = {
  create_option_on_input_new?: boolean;
  data_type: string;
  description?: string;
  i18n_name?: string;
  id: string;
  max_value?: null | number;
  metadata?: Record<string, unknown>;
  min_value?: null | number;
  name: string;
  options?: Record<string, unknown>[];
  unit?: null | string;
  [key: string]: unknown;
};

/** `_AuditEvent` — stream class `audit`. */
export type AuditEvent = {
  action: "sign_in" | "sign_out" | "authorize" | "identity_admin" | "credential_admin" | "execute" | "rebuild" | "restore" | "security_config";
  actor_id?: null | string;
  actor_kind: "human" | "service" | "node" | "system" | "anonymous";
  actor_label?: null | string;
  changed_fields?: string[];
  correlation_id?: null | string;
  entity_id?: null | string;
  entity_type?: null | string;
  event_id: string;
  metadata?: Record<string, unknown>;
  occurred_at: number;
  operation?: null | string;
  outcome: "success" | "failure" | "denied";
  reason_code?: null | string;
  source: "colca" | "api" | "keycloak" | "projector" | "node_manager";
  [key: string]: unknown;
};

/** `_Cmd` — stream class `cmd`. */
export type Cmd = {
  command?: Record<string, unknown>;
  correlation_id: string;
  created_at?: number;
  expires_at: number;
  [key: string]: unknown;
};

/** `_CmdAdmin` — stream class `cmd`. */
export type CmdAdmin = {
  command?: Record<string, unknown>;
  correlation_id: string;
  created_at?: number;
  expires_at: number;
  [key: string]: unknown;
};

/** `_CmdConfigure` — stream class `cmd`. */
export type CmdConfigure = {
  command?: Record<string, unknown>;
  correlation_id: string;
  created_at?: number;
  expires_at: number;
  [key: string]: unknown;
};

/** `_CmdEdit` — stream class `cmd`. */
export type CmdEdit = {
  command?: Record<string, unknown>;
  correlation_id: string;
  created_at?: number;
  expected_versions: Record<string, unknown>;
  expires_at: number;
  intent: Record<string, unknown>;
  operation_id: string;
  [key: string]: unknown;
};

/** `_CmdMaintain` — stream class `cmd`. */
export type CmdMaintain = {
  command?: Record<string, unknown>;
  correlation_id: string;
  created_at?: number;
  expires_at: number;
  [key: string]: unknown;
};

/** `_CmdOperate` — stream class `cmd`. */
export type CmdOperate = {
  command?: Record<string, unknown>;
  correlation_id: string;
  created_at?: number;
  expires_at: number;
  [key: string]: unknown;
};

/** `_CmdParam` — stream class `cmd`. */
export type CmdParam = {
  command?: Record<string, unknown>;
  correlation_id: string;
  created_at?: number;
  expires_at: number;
  [key: string]: unknown;
};

/** `_Constant` — stream class `entity`. */
export type Constant = {
  created_at?: null | string;
  data_type: "float64" | "int64" | "boolean" | "string" | "datetime" | "json";
  description?: string;
  id: string;
  metadata?: Record<string, unknown>;
  name: string;
  precision?: null | number;
  semantic_type_id?: null | string;
  system_element_id?: null | string;
  unit?: null | string;
  updated_at?: null | string;
  value?: unknown;
  [key: string]: unknown;
};

/** `_DataModel` — stream class `definition`. */
export type DataModel = {
  description?: string;
  extends?: string[];
  id: string;
  name: string;
  slots?: Record<string, unknown>[];
  version?: string;
  [key: string]: unknown;
};

/** `_DataTags` — stream class `entity`. */
export type DataTags = {
  connector: string;
  data_tags: {
    data_type?: null | string;
    id: string;
    is_readable: boolean;
    is_stale?: boolean;
    is_writable: boolean;
    meta?: Record<string, unknown>;
    name: string;
    source: string;
    [key: string]: unknown;
  }[];
  [key: string]: unknown;
};

/** `_EditOperation` — stream class `entity`. */
export type EditOperation = {
  digest: string;
  id: string;
  message: string;
  result: string;
  topics: string[];
  [key: string]: unknown;
};

/** `_ExternalReference` — stream class `entity`. */
export type ExternalReference = {
  description?: string;
  external_column?: string;
  external_row_id: string;
  external_system_id: string;
  external_table: string;
  id: string;
  relationship_type: string;
  source_entity: string;
  source_object_id: string;
  [key: string]: unknown;
};

/** `_ExternalSystem` — stream class `definition`. */
export type ExternalSystem = {
  description?: string;
  id: string;
  key: string;
  name: string;
  properties?: Record<string, unknown>;
  system_type: string;
  [key: string]: unknown;
};

/** `_Group` — stream class `definition`. */
export type Group = {
  description?: string;
  grants?: string[];
  id: string;
  name: string;
  [key: string]: unknown;
};

/** `_Log` — stream class `log`. */
export type Log = {
  exc_info?: null | string;
  extra?: null | Record<string, unknown>;
  function: string;
  level: string;
  line_no: number;
  logger_name: string;
  message: string;
  module: string;
  timestamp: string;
  [key: string]: unknown;
};

/** `_MetadataType` — stream class `definition`. */
export type MetadataType = {
  allowed_content_type_keys?: string[];
  data_type: string;
  description?: string;
  i18n_name?: string;
  id: string;
  is_mandatory?: boolean;
  name: string;
  [key: string]: unknown;
};

/** `_Metric` — stream class `data`. */
export type Metric = {
  colca_node_id?: null | string;
  error?: null | string;
  signal_id: string;
  timestamp: number;
  value: unknown;
  [key: string]: unknown;
};

/** `_Node` — stream class `entity`. */
export type Node = {
  description?: string;
  display_name?: string;
  health_metrics?: {
    description?: string;
    key: string;
    max_value?: null | number;
    metric: string;
    min_value?: null | number;
    name: string;
    precision?: null | number;
    query?: string;
    thresholds?: Record<string, unknown>;
    unit?: string;
    visualization?: "gauge" | "timeline" | "value";
  }[];
  id: string;
  metadata?: Record<string, unknown>;
  name: string;
  network_interfaces?: {
    addresses?: string[];
    interface_type: "ethernet" | "wifi" | "cellular" | "loopback" | "bridge" | "vpn" | "overlay" | "virtual" | "other";
    mac_address?: string;
    name: string;
    observed_at?: number;
  }[];
  root_system_element_id?: null | string;
  [key: string]: unknown;
};

/** `_NotificationConfigStatus` — stream class `entity`. */
export type NotificationConfigStatus = {
  applied_at: number;
  config_id: string;
  id: string;
  message?: null | string;
  reason_code?: null | string;
  revision_id: string;
  status: string;
  target_node_id: string;
  [key: string]: unknown;
};

/** `_NotificationDispatched` — stream class `alarm`. */
export type NotificationDispatched = {
  alarm_event_id: null | string;
  at: number;
  attempt: number;
  channel_id: string;
  channel_kind?: null | string;
  error?: null | string;
  idempotency_key: string;
  latency_ms: null | number;
  metadata_json?: Record<string, unknown>;
  policy_id: null | string;
  provider?: null | string;
  provider_message_id?: null | string;
  recipient_id: string;
  retryable?: boolean;
  status: string;
  terminal?: boolean;
  [key: string]: unknown;
};

/** `_PersonalAccessToken` — stream class `definition`. */
export type PersonalAccessToken = {
  expires_at?: null | string;
  grants?: string[];
  hashed_secret: string;
  id: string;
  namespace_read_permissions?: string[];
  namespace_write_permissions?: string[];
  owner_email: string;
  owner_sub: string;
  roles?: string[];
  scopes?: string[];
  [key: string]: unknown;
};

/** `_Resource` — stream class `entity`. */
export type Resource = {
  content_type?: string;
  created_at?: null | string;
  description?: string;
  display_name?: string;
  filename: string;
  id: string;
  metadata?: Record<string, unknown>;
  resource_type?: string;
  sha256?: string;
  size_bytes?: number;
  system_element_id: string;
  updated_at?: null | string;
  [key: string]: unknown;
};

/** `_SemanticTag` — stream class `definition`. */
export type SemanticTag = {
  applies_to?: string[];
  data_type?: null | string;
  description?: string;
  i18n_name?: string;
  icon?: string;
  id: string;
  name: string;
  quantity_kind?: null | string;
  [key: string]: unknown;
};

/** `_ServiceDetails` — stream class `entity`. */
export type ServiceDetails = {
  architecture_metadata?: Record<string, unknown>;
  colca_node_id: string;
  description?: string;
  display_name?: string;
  health_metrics?: {
    description?: string;
    key: string;
    max_value?: null | number;
    metric: string;
    min_value?: null | number;
    name: string;
    precision?: null | number;
    query?: string;
    thresholds?: Record<string, unknown>;
    unit?: string;
    visualization?: "gauge" | "timeline" | "value";
  }[];
  hierarchy?: string[];
  id: string;
  is_active?: boolean;
  metadata?: Record<string, unknown>;
  name: string;
  service_type: "connector" | "notifications" | "platform-health" | "ui" | "database" | "api" | "grafana" | "broker" | "agent" | "dataops" | "reverse-proxy" | "auth-service" | "mcp-server" | "docs" | "database-viewer" | "ca" | "advertiser" | "other" | "unknown";
  system_element_id?: null | string;
  [key: string]: unknown;
};

/** `_Signal` — stream class `entity`. */
export type Signal = {
  config?: Record<string, unknown>;
  created_at?: null | string;
  data_tag?: null | string;
  data_type?: "int" | "float" | "string" | "boolean" | "datetime" | "int_array" | "float_array" | "string_array" | "boolean_array" | "datetime_array" | null;
  description?: string;
  has_contract?: boolean;
  id: string;
  index_type?: "time" | "numerical" | "none" | null;
  is_logged?: boolean;
  is_published?: boolean;
  max_value?: null | number;
  metadata?: Record<string, unknown>;
  min_value?: null | number;
  name: string;
  precision?: null | number;
  replication_policy?: "replicate_to_parents" | "source_local_only";
  semantic_type_id?: null | string;
  system_element_id?: null | string;
  unit?: null | string;
  updated_at?: null | string;
  [key: string]: unknown;
};

/** `_SystemElement` — stream class `entity`. */
export type SystemElement = {
  created_at?: null | string;
  description?: string;
  external_asset_id?: null | string;
  external_asset_id_type?: null | string;
  id: string;
  implements?: string[];
  metadata?: Record<string, unknown>;
  name: string;
  parent_id?: null | string;
  semantic_type_id?: null | string;
  updated_at?: null | string;
  [key: string]: unknown;
};

/** Every contract the node knows, with the stream it lands on. */
export const CONTRACTS = {
  "_Ack": { class: "ack", tombstone: false },
  "_AlarmNotificationConfig": { class: "entity", tombstone: true },
  "_AlarmState": { class: "entity", tombstone: true },
  "_AlarmStateChange": { class: "alarm", tombstone: false },
  "_Annotation": { class: "annotation", tombstone: false },
  "_AnnotationType": { class: "definition", tombstone: true },
  "_AuditEvent": { class: "audit", tombstone: false },
  "_Cmd": { class: "cmd", tombstone: false },
  "_CmdAdmin": { class: "cmd", tombstone: false },
  "_CmdConfigure": { class: "cmd", tombstone: false },
  "_CmdEdit": { class: "cmd", tombstone: false },
  "_CmdMaintain": { class: "cmd", tombstone: false },
  "_CmdOperate": { class: "cmd", tombstone: false },
  "_CmdParam": { class: "cmd", tombstone: false },
  "_Constant": { class: "entity", tombstone: true },
  "_DataModel": { class: "definition", tombstone: true },
  "_DataTags": { class: "entity", tombstone: true },
  "_EditOperation": { class: "entity", tombstone: true },
  "_ExternalReference": { class: "entity", tombstone: true },
  "_ExternalSystem": { class: "definition", tombstone: true },
  "_Group": { class: "definition", tombstone: true },
  "_Log": { class: "log", tombstone: false },
  "_MetadataType": { class: "definition", tombstone: true },
  "_Metric": { class: "data", tombstone: true },
  "_Node": { class: "entity", tombstone: true },
  "_NotificationConfigStatus": { class: "entity", tombstone: true },
  "_NotificationDispatched": { class: "alarm", tombstone: false },
  "_PersonalAccessToken": { class: "definition", tombstone: true },
  "_Resource": { class: "entity", tombstone: true },
  "_SemanticTag": { class: "definition", tombstone: true },
  "_ServiceDetails": { class: "entity", tombstone: true },
  "_Signal": { class: "entity", tombstone: true },
  "_SystemElement": { class: "entity", tombstone: true },
} as const;

/** The wire names, `_Metric` and friends. */
export type ContractName = keyof typeof CONTRACTS;

/** The payload type behind a contract name: `PayloadOf<"_Metric">`. */
export interface PayloadByContract {
  "_Ack": Ack;
  "_AlarmNotificationConfig": AlarmNotificationConfig;
  "_AlarmState": AlarmState;
  "_AlarmStateChange": AlarmStateChange;
  "_Annotation": Annotation;
  "_AnnotationType": AnnotationType;
  "_AuditEvent": AuditEvent;
  "_Cmd": Cmd;
  "_CmdAdmin": CmdAdmin;
  "_CmdConfigure": CmdConfigure;
  "_CmdEdit": CmdEdit;
  "_CmdMaintain": CmdMaintain;
  "_CmdOperate": CmdOperate;
  "_CmdParam": CmdParam;
  "_Constant": Constant;
  "_DataModel": DataModel;
  "_DataTags": DataTags;
  "_EditOperation": EditOperation;
  "_ExternalReference": ExternalReference;
  "_ExternalSystem": ExternalSystem;
  "_Group": Group;
  "_Log": Log;
  "_MetadataType": MetadataType;
  "_Metric": Metric;
  "_Node": Node;
  "_NotificationConfigStatus": NotificationConfigStatus;
  "_NotificationDispatched": NotificationDispatched;
  "_PersonalAccessToken": PersonalAccessToken;
  "_Resource": Resource;
  "_SemanticTag": SemanticTag;
  "_ServiceDetails": ServiceDetails;
  "_Signal": Signal;
  "_SystemElement": SystemElement;
}

export type PayloadOf<T extends ContractName> = PayloadByContract[T];
