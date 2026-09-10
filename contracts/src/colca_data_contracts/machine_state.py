"""Ordinal enums for the platform machine-state contract.

The integers behind the ``MachineState`` data model
(``data_models/machinestate.yaml``). Its ``enum`` slot values match these
``IntEnum``s name for name and index for index; ``test_machine_state.py``
checks that.

They are ints rather than strings because the state is stored as a number,
which is what time-in-state queries bucket on.
"""

from enum import IntEnum


class MachineState(IntEnum):
    """Axis 1: what the asset is doing, the input to OEE (ISO 22400).

    Mutually exclusive, not a bitfield. OPC UA Machinery ``MachineryItemState``
    plus ``UNKNOWN``; an observation, not a state machine with transitions.
    """

    UNKNOWN = 0  # state cannot be determined; uncovered time for OEE, not a loss
    OFFLINE = 1  # de-energized or not scheduled (OPC UA NotAvailable); needs a power signal, not a stale heartbeat
    DOWN = 2  # available but not functional: fault, trip, e-stop; the cause is a StateReason
    IDLE = 3  # available & healthy, performing no activity (NotExecuting)
    EXECUTING = 4  # producing (OPC UA Executing)


class OperatingMode(IntEnum):
    """Axis 2: who drives the machine, planned or unplanned. Kept apart from
    MachineState; with ``UNKNOWN``, OEE falls back to the shift calendar.
    """

    UNKNOWN = 0
    PRODUCTION = 1
    SETUP = 2
    MAINTENANCE = 3
    MANUAL = 4
    ENGINEERING = 5


class StateReason(IntEnum):
    """Axis 3: the cause, which routes an interval to an OEE loss family.
    ``UNCLASSIFIED`` is the default and is never guessed.
    """

    UNCLASSIFIED = 0  # honest default; OEE routes conservatively to an unplanned stop
    LOCAL_FAULT = 1  # internal fault, charged to this machine's availability and MTBF/MTTR
    EMERGENCY_STOP = 2  # e-stop, safety or guard; its own MTTR, legally sensitive
    STARVED = 3  # no infeed from upstream; attributed to the line
    BLOCKED = 4  # outfeed full, downstream cannot accept; attributed to the line
    UTILITY_LOSS = 5  # lost shared air / steam / power
    CHANGEOVER = 6  # format / recipe / tool change; planned when operating_mode=SETUP
    CLEANING = 7  # CIP / SIP / washdown; planned
    PLANNED_MAINTENANCE = 8  # scheduled PM / calibration; excluded from the Availability denominator
    NO_DEMAND = 9  # capable but idle, no order by plan
    NO_SHIFT = 10  # outside scheduled time; the one planned bucket derivable from calendar alone
    WARMUP = 11  # startup ramp before steady state; feeds Startup-Reject quality loss
    QUALITY_HOLD = 12  # stopped / holding on an off-spec condition
    GRADE_CHANGE = 13  # continuous-process transition without a stop; output is off-spec meanwhile
