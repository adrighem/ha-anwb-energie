"""Split the electricity export credit into market value and netting refund.

Kraken books the export credit as negative ``GENERATION`` cost lines. The
sensors expose a positive credit, so every booked amount is negated here.
Days after the last booked day are unbooked and priced by the coordinator.
"""

from __future__ import annotations

from dataclasses import dataclass

from .kraken_costs import KrakenPeriodCosts


@dataclass(frozen=True)
class ExportCreditBreakdown:
    """Export credit components in euro, positive for money received.

    A booked component is ``None`` when Kraken's label split is unknown.
    ``unbooked_credit`` is ``None`` when an unbooked exported hour lacks a
    tariff. When all are known they add up to the export credit.
    """

    booked_market_value: float | None
    booked_net_metering_refund: float | None
    booked_other: float | None
    unbooked_kwh: float
    unbooked_credit: float | None

    def as_attributes(self) -> dict[str, float | None]:
        """Return rounded sensor attributes without negative zeros."""
        return {
            "booked_market_value": _round(self.booked_market_value, 2),
            "booked_net_metering_refund": _round(self.booked_net_metering_refund, 2),
            "booked_other": _round(self.booked_other, 2),
            "unbooked_kwh": _round(self.unbooked_kwh, 3),
            "unbooked_credit": _round(self.unbooked_credit, 2),
        }


def _round(value: float | None, digits: int) -> float | None:
    # Adding 0.0 turns a rounded -0.0 into 0.0.
    return round(value, digits) + 0.0 if value is not None else None


def _credit(amount: float | None) -> float | None:
    return -amount if amount is not None else None


def booked_breakdown(
    period: KrakenPeriodCosts,
    *,
    unbooked_kwh: float,
    unbooked_credit: float | None,
) -> ExportCreditBreakdown:
    """Return the credit components of booked Kraken days plus unbooked days."""
    return ExportCreditBreakdown(
        booked_market_value=_credit(period.market_value),
        booked_net_metering_refund=_credit(period.net_metering_refund),
        booked_other=_credit(period.other_variable_cost),
        unbooked_kwh=unbooked_kwh,
        unbooked_credit=unbooked_credit,
    )


def booked_market_value(
    period: KrakenPeriodCosts,
    unbooked_market_value: float | None,
) -> float | None:
    """Return booked plus unbooked market value, or ``None`` when unknown."""
    if period.market_value is None or unbooked_market_value is None:
        return None
    return -period.market_value + unbooked_market_value
