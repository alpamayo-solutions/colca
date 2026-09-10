"""The curated Material Symbols Outlined ligature set.

The font subset keeps exactly these glyphs, the semantic-tag picker offers
exactly these, and the command boundary refuses a tag whose icon is not in the
set, so a typo cannot render as literal text. A name Material Symbols does not
have fails the subset build.
"""

# Entity-kind defaults and system-element tags.
_STRUCTURE = {
    "domain",
    "location_city",
    "grid_view",
    "linear_scale",
    "widgets",
    "precision_manufacturing",
    "account_tree",
    "factory",
    "warehouse",
    "conveyor_belt",
    "settings_input_component",
    "hub",
    "lan",
}

# Measurements and the quantity kinds.
_MEASUREMENT = {
    "thermostat",
    "compress",
    "water_drop",
    "waves",
    "opacity",
    "humidity_percentage",
    "bolt",
    "electrical_services",
    "power",
    "electric_meter",
    "rotate_right",
    "speed",
    "build",
    "fitness_center",
    "vibration",
    "straighten",
    "scale",
    "inventory_2",
    "graphic_eq",
    "numbers",
    "timer",
    "schedule",
    "offline_bolt",
    "electric_bolt",
    "slow_motion_video",
    "arrow_upward",
    "inventory",
    "monitor_weight",
    "air",
    "gas_meter",
    "water",
    "valve",
    "sensors",
    "pulse_alert",
}

# States, identity and general UI.
_GENERAL = {
    "list_alt",
    "info",
    "monitor_heart",
    "notification_important",
    "crisis_alert",
    "data_object",
    "edit_note",
    "tag",
    "event",
    "label",
    "check_circle",
    "cancel",
    "warning",
    "error",
    "pending",
    "toggle_on",
    "toggle_off",
    "visibility",
    "visibility_off",
    "lock",
    "lock_open",
    "link",
    "link_off",
    "search",
    "filter_alt",
    "sort",
    "settings",
    "tune",
    "add",
    "remove",
    "edit",
    "delete",
    "content_copy",
    "save",
    "history",
    "schema",
    "category",
    "folder",
    "description",
    "science",
    "engineering",
    "handyman",
    "construction",
    "manage_history",
    "trending_up",
    "trending_down",
    "show_chart",
    "bar_chart",
    "query_stats",
    "calculate",
    "functions",
    "straight",
    "route",
}

# Glyphs the editor's property badges use (data type, source, file type).
# Listing them here makes test_icon_set_ships check that the font has them.
_PROPERTY_BADGES = {
    "image",
    "table",
    "folder_zip",
    "memory",
}

ICON_SET: frozenset[str] = frozenset(_STRUCTURE | _MEASUREMENT | _GENERAL | _PROPERTY_BADGES)
