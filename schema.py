"""Single source of truth for the raw log format and validity rules.

Both the generator (which injects defects) and the compactor (which detects them)
import from here, so "what counts as bad" can never drift between the two.

Raw line format (CSV, no header, no quoting, one reading per line):

    2026-09-22T02:13:21.400,7,dissolved_oxygen,6.82

    timestamp  ISO-8601, exactly YYYY-MM-DDTHH:MM:SS.mmm, naive, assumed UTC
    tank_id    integer in TANK_ID_RANGE
    metric     one of METRICS (case-sensitive)
    value      finite decimal number inside VALID_RANGES[metric]
"""

MEMORY_CEILING_MB = 512

METRICS = ("dissolved_oxygen", "temperature")

# Physically plausible bounds. A reading outside them parses fine but is rejected
# as "out_of_range" (a broken probe, not a fish in boiling water).
VALID_RANGES = {
    "dissolved_oxygen": (0.0, 20.0),  # mg/L
    "temperature": (-2.0, 40.0),  # degrees C
}

TANK_ID_RANGE = (1, 999)

TIMESTAMP_LEN = len("2026-09-22T02:13:21.400")

# Every rejected line gets exactly one of these reasons.
REJECT_REASONS = (
    "line_too_long",
    "empty_line",
    "wrong_field_count",
    "bad_timestamp",
    "bad_tank_id",
    "unknown_metric",
    "bad_value",
    "out_of_range",
)
