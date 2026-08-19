from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TableNames:
    catalog: str = "mapletrade_dev"

    @property
    def bronze_events(self) -> str:
        return f"{self.catalog}.bronze.trade_events"

    @property
    def instruments(self) -> str:
        return f"{self.catalog}.reference.instruments"

    @property
    def portfolios(self) -> str:
        return f"{self.catalog}.reference.portfolios"

    @property
    def fx_rates(self) -> str:
        return f"{self.catalog}.reference.fx_rates"

    @property
    def ingestion_manifest(self) -> str:
        return f"{self.catalog}.reference.ingestion_manifest"

    @property
    def outcomes(self) -> str:
        return f"{self.catalog}.silver.trade_event_outcomes"

    @property
    def current(self) -> str:
        return f"{self.catalog}.silver.trade_current"

    @property
    def booked_view(self) -> str:
        return f"{self.catalog}.gold.vw_valid_booked_trade"

    @property
    def positions(self) -> str:
        return f"{self.catalog}.gold.position_snapshot"

    @property
    def currency_notional(self) -> str:
        return f"{self.catalog}.gold.net_traded_notional_by_currency"

    @property
    def reconciliation(self) -> str:
        return f"{self.catalog}.ops.pipeline_reconciliation"

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/ops/pipeline_state"
