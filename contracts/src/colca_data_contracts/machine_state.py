"""MachineState — ordinal enums for the platform machine-state contract.

Single source of truth for the integers behind the ``MachineState`` interface
(``interfaces/machinestate.yaml``). The YAML ``enum_values`` lists MUST match
these ``IntEnum``s name-for-name and index-for-index; ``test_machine_state.py``
asserts that, so the contract and the code can never drift.

Why ordinal ints (not a StrEnum): ``machine_state`` is stored in
``Signal.value_number`` so Grafana time-in-state bucketing survives — the
bucketed/gap-fill query renders a string state as empty (grafana-plugin
``db.go``). The IntEnum value IS the stored int.

Why this lives in colca-data-contracts (not dataops): the contract YAML's
``enum_values`` derive from these enums, and the api service / UI / dm all consume the
contract. ``dataops`` depends on ``colca-data-contracts``, never the reverse —
so the MachineState producer base imports these from here.

These are NOT a ``value_json`` semantic contract (see ``semantic/``); they are
the value vocabulary for ordinal-int signals, so they are intentionally not
registered in ``SEMANTIC_CONTRACTS``.
"""

from enum import IntEnum


class MachineState(IntEnum):
    """Axis 1 — WHAT the asset is doing. The sole OEE/ISO-22400 input.

    Mutually exclusive (NOT a bitfield). OPC-UA Machinery ``MachineryItemState``
    roll-up + a Colca ``UNKNOWN``. No transition guards — this is an
    observation roll-up, not a behavioural state machine.
    """

    UNKNOWN = 0    # honesty sentinel — cannot determine state; OEE uncovered time, not a loss bucket
    OFFLINE = 1    # de-energized / not scheduled (OPC-UA NotAvailable). Needs a positive power/disconnect signal — never inferred from a stale heartbeat.
    DOWN = 2       # available but not functional — fault / trip / e-stop / externally blocked. Cause lives in StateReason.
    IDLE = 3       # available & healthy, performing no activity (NotExecuting)
    EXECUTING = 4  # actively pursuing its purpose (Executing). The single producing anchor; part-counter-free.


class OperatingMode(IntEnum):
    """Axis 2 — WHY / who drives. Planned-vs-unplanned driver consumed by the
    OEE layer (which owns the shift calendar). NEVER folded into MachineState.
    ``UNKNOWN`` => OEE falls back to the shift calendar.
    """

    UNKNOWN = 0
    PRODUCTION = 1
    SETUP = 2
    MAINTENANCE = 3
    MANUAL = 4
    ENGINEERING = 5


class StateReason(IntEnum):
    """Axis 3 — CAUSE. Routes each interval to an OEE loss family
    deterministically; carries the local-vs-upstream (LOCAL_FAULT vs
    STARVED/BLOCKED) and starved-vs-blocked splits. ``UNCLASSIFIED`` is the
    honest default and is never guessed.
    """

    UNCLASSIFIED = 0         # honest default; OEE routes conservatively to an unplanned stop
    LOCAL_FAULT = 1          # internal fault — charged to THIS machine's Availability + MTBF/MTTR
    EMERGENCY_STOP = 2       # e-stop / safety / guard — distinct MTTR, legally sensitive
    STARVED = 3              # no infeed from upstream — external, attributed to the line
    BLOCKED = 4              # outfeed full / downstream cannot accept — external, attributed to the line
    UTILITY_LOSS = 5         # lost shared air / steam / power
    CHANGEOVER = 6           # format / recipe / tool change; planned when operating_mode=SETUP
    CLEANING = 7             # CIP / SIP / washdown; planned
    PLANNED_MAINTENANCE = 8  # scheduled PM / calibration; excluded from the Availability denominator
    NO_DEMAND = 9            # capable but idle, no order by plan
    NO_SHIFT = 10            # outside scheduled time; the one planned bucket derivable from calendar alone
    WARMUP = 11              # startup ramp before steady state; feeds Startup-Reject quality loss
    QUALITY_HOLD = 12        # stopped / holding on an off-spec condition
    GRADE_CHANGE = 13        # continuous-process transition with NO stop (runs while transitioning, output off-spec)
