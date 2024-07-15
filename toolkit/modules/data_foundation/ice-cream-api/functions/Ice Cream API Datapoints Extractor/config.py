from dataclasses import dataclass, field
from typing import List

from cognite.extractorutils.configtools import BaseConfig, RawStateStoreConfig, StateStoreConfig


@dataclass
class ApiConfig:
    url: str

@dataclass
class ExtractorConfig:
    backfill: bool
    data_set_ext_id: str
    days: int
    sites: List[str]
    state_store: StateStoreConfig = field(
        default_factory=StateStoreConfig(
            raw=RawStateStoreConfig(database=None, table=None)
        )
    )

@dataclass
class Config(BaseConfig):
    api: ApiConfig
    extractor: ExtractorConfig

# @dataclass
# class BackFillConfig:
#     enabled: bool
#     history_days: int


# @dataclass
# class FrontFillConfig:
#     enabled: bool
#     continuous: bool
#     lookback_min: float


# @dataclass
# class IceCreamFactoryConfig(BaseConfig):
#     # backfill: BackFillConfig
#     # frontfill: FrontFillConfig
#     # oee_timeseries_dataset_ext_id: str  # ext id of dataset for oee timeseries. Used to populate timeseries in the correct dataset
#     extractor: ExtractorConfig
