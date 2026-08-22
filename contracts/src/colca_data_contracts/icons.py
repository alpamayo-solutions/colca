"""The curated Material Symbols Outlined ligature set.

One list, three consumers: the self-hosted font subset build retains exactly
these glyphs, the semantic-tag editor's picker offers exactly these options,
and the command boundary refuses a tag whose icon is outside it. An authored
typo is therefore unrepresentable rather than rendering as literal text in the
entity tree for everyone downstream.

A name that does not exist in Material Symbols fails the subset build, so
invalid entries are caught at build time rather than by a user.
"""

# Entity-kind defaults and system-element tags.
_STRUCTURE = {
    "domain", "location_on", "grid_view", "linear_scale", "widgets",
    "precision_manufacturing", "account_tree", "factory", "warehouse",
    "conveyor_belt", "settings_input_component", "hub", "lan",
}

# Measurements and the quantity kinds.
_MEASUREMENT = {
    "thermostat", "compress", "water_drop", "waves", "opacity",
    "humidity_percentage", "bolt", "electrical_services", "power",
    "electric_meter", "rotate_right", "speed", "build", "fitness_center",
    "vibration", "straighten", "scale", "inventory_2", "graphic_eq",
    "numbers", "timer", "schedule", "offline_bolt", "electric_bolt",
    "slow_motion_video", "arrow_upward", "inventory", "monitor_weight",
    "air", "gas_meter", "water", "valve", "sensors", "pulse_alert",
}

# States, identity and general UI.
_GENERAL = {
    "list_alt", "info", "monitor_heart", "notification_important",
    "crisis_alert", "data_object", "edit_note", "tag", "event", "label",
    "check_circle", "cancel", "warning", "error", "pending", "toggle_on",
    "toggle_off", "visibility", "visibility_off", "lock", "lock_open",
    "link", "link_off", "search", "filter_alt", "sort", "settings",
    "tune", "add", "remove", "edit", "delete", "content_copy", "save",
    "history", "schema", "category", "folder", "description", "science",
    "engineering", "handyman", "construction", "manage_history",
    "trending_up", "trending_down", "show_chart", "bar_chart",
    "query_stats", "calculate", "functions", "straight", "route",
}

ICON_SET: frozenset[str] = frozenset(_STRUCTURE | _MEASUREMENT | _GENERAL)
