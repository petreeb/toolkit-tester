from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from cognite.extractorutils.configtools import BaseConfig
from cognite.extractorutils.configtools import RawStateStoreConfig
from cognite.extractorutils.configtools import StateStoreConfig


@dataclass
class ApiConfig:
    url: str

@dataclass
class ExtractorConfig:
    data_set_ext_id: str
    upload_interval: int
    state_store: StateStoreConfig = field(
        default_factory=StateStoreConfig(
            raw=RawStateStoreConfig(database=None, table=None)
        )
    )

@dataclass
class Config(BaseConfig):
    api: ApiConfig
    # backfill: BackFillConfig
    # frontfill: FrontFillConfig
    # oee_timeseries_dataset_ext_id: str  # ext id of dataset for oee timeseries. Used to populate timeseries in the correct dataset
    extractor: ExtractorConfig
