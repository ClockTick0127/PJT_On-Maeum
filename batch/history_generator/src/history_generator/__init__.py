"""Rule-based historical lake data generator.

The aggregation pipeline reads two independent inputs from the lake: raw power
(coverage only) and appliance sessions (every usage metric). This package writes
both, plus analysis receipts, for a date range from a scenario JSON, so that
``power-silver`` / ``gold-profile`` / ``household-report`` can be run over months
of past dates without the realtime services.
"""

__all__ = ["__version__"]
__version__ = "0.1.0"
