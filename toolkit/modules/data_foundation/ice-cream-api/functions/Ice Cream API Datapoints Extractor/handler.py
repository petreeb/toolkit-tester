from datetime import datetime, timedelta, timezone
from threading import Event
from typing import Union, Sequence
import yaml

from cognite.client import CogniteClient
from cognite.extractorutils import Extractor
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.uploader import TimeSeriesUploadQueue

from config import Config
from ice_cream_factory_api import IceCreamFactoryAPI


def get_timeseries_for_site(client: CogniteClient, site, config: Config):
    this_site = site.lower()
    print(f"Getting TimeSeries for {site}")
    ts = client.time_series.list(
        data_set_external_ids=config.extractor.data_set_ext_id,
        metadata={"site": this_site},
        limit=None
    )


    # filter returned list because the API returns connected timeseries. planned_status -> status, good -> count
    ts = [item for item in ts if any(substring in item.external_id for substring in ["planned_status", "good"])]
    return ts

def run_extractor(
    client: CogniteClient, states: AbstractStateStore, config: Config, stop_event: Event
) -> None:

    now = datetime.now(timezone.utc).timestamp() * 1000
    increment = timedelta(seconds=7200).total_seconds() * 1000
    
    ice_cream_api = IceCreamFactoryAPI(base_url=config.api.url)
    
    upload_queue = TimeSeriesUploadQueue(
        client,
        post_upload_function=states.post_upload_handler(),
        max_queue_size=100000,
        trigger_log_level="INFO",
        thread_name="Timeseries Upload Queue",
    )

    for site in config.extractor.sites:
        total_dps = 0
        time_series = get_timeseries_for_site(client, site, config)

        ts_external_ids = [ts.external_id for ts in time_series]

        latest_dps = {
            dp.external_id: dp.timestamp
            for dp in client.time_series.data.retrieve_latest(
                external_id=ts_external_ids,
                ignore_unknown_ids=True
            )
        }

        for ts in time_series:
            latest = latest_dps[ts.external_id][0] if ts.external_id in latest_dps and latest_dps[ts.external_id] else None
            start = latest if latest else now - increment
            end = start + increment

            while start <= now and not stop_event.is_set():
                dps = ice_cream_api.get_datapoints(timeseries_ext_id=ts.external_id, start=start, end=end)
                for external_id, datapoints in dps.items():
                    upload_queue.add_to_upload_queue(external_id=external_id, datapoints=datapoints)
                    total_dps += len(datapoints)

                start += increment
                end += increment

            # trigger upload for this timeseries
        upload_queue.upload()
        print(f"Finished uploading {total_dps} datapoints for {site}")

def handle(client: CogniteClient = None, data = None):
    config_file_path = "extractor_config.yaml"

    if data:
        sites = data.get("sites")
        backfill = data.get("backfill")
        days = data.get("days")

        with open(config_file_path) as config_file:
            config = yaml.safe_load(config_file)
        
        extractor_config = config["extractor"]

        if sites:
            extractor_config["sites"] = sites
        if backfill:
            extractor_config["backfill"] = backfill
            if days:
                extractor_config["days"] = days

        with open(config_file_path, 'w') as outfile:
            yaml.dump(config, outfile)

    with Extractor(
        name="Ice Cream API Assets Extractor",
        description="An extractor that ingest Timeseries' Datapoints from the Ice Cream Factory API to CDF clean",
        config_class=Config,
        version="1.0",
        config_file_path=config_file_path,
        run_handle=run_extractor,
    ) as extractor:
        extractor.run()
    
if __name__ == "__main__":
    handle(data={"sites": ["Oslo", "Houston"]})