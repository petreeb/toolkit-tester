from __future__ import annotations

import argparse
import logging
import os
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Dict, List

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.extractorutils import Extractor
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from cognite.extractorutils.util import ensure_time_series

from config import Config
from datapoints_backfiller import Backfiller
from datapoints_streamer import Streamer
from ice_cream_factory_api import IceCreamFactoryAPI

logging.basicConfig(level=logging.INFO)

def timeseries_updates(
    timeseries_list: List[TimeSeries], config: Config, client: CogniteClient
) -> List[TimeSeries]:
    """
    Update Timeseries object with dataset_id and asset_id. This is so non-existing timeseries get created with
    the needed data in the ensure_time_series function from extractorutils.

    Args:
        timeseries_list: List of timeseries
        config: Config data for this extractor
        client: Cognite client

    Returns:
        updated_timeseries_list: List of updated timeseries
    """

    asset_ext_ids_set = set([ts.external_id.split(":")[0] for ts in timeseries_list])

    # get asset data from CDF
    cdf_assets = client.assets.retrieve_multiple(external_ids=list(asset_ext_ids_set), ignore_unknown_ids=True)
    asset_ext_id_to_id_dict = {asset.external_id: asset.id for asset in cdf_assets}

    ds = client.data_sets.retrieve(external_id=config.data_set_ext_id)
    if ds:
        ts_data_set_id = ds.id
    else:
        logging.info("Could not find existing dataset. Have you run bootstrap cli?")
        return

    updated_timeseries_list: List[TimeSeries] = []
    for timeseries in timeseries_list:
        timeseries.data_set_id = ts_data_set_id
        timeseries.asset_id = asset_ext_id_to_id_dict.get(timeseries.external_id.split(":")[0])
        updated_timeseries_list.append(timeseries)

    return updated_timeseries_list


def run_extractor(
    client: CogniteClient, states: AbstractStateStore, config: Config, stop_event: Event, data: Dict = None
) -> None:
    """
    Run extractor and extract datapoints for timeseries for sites given in config.

    Args:
        client: Initialized CogniteClient object
        states: Initialized state store object
        config: Configuration parameters
        stop_event: Cancellation token, will be set when an interrupt signal is sent to the extractor process
    """
    print(client.config)
    if data:
        temp_sites = data.get("sites", None)

        if temp_sites:
            # Check for proper format
            if isinstance(temp_sites, List):
                sites = temp_sites
            else:
                logging.error(f"sites input is incorrect format {temp_sites}")
                return
    else:
        sites = [site.external_id for site in client.assets.list(root=True, data_set_external_ids=config.extractor.data_set_ext_id, limit=None)]
    print(sites)

    logging.info("Starting Ice Cream Factory datapoints extractor")
    ice_cream_api = IceCreamFactoryAPI(base_url=config.api.url)

    timeseries_list = []
    for site in sites:
        assets_subtree = [asset.external_id for asset in client.assets.retrieve_subtree(external_id=site)]
        timeseries_list.append(assets_subtree)

    if not timeseries_list:
        logging.warning("The Time Series requested do not exist")
        stop_event.set()
        return
    # timeseries_list = timeseries_updates(timeseries_list=oee_timeseries_list, config=config, client=client)

    # if timeseries_list:
    # Only request datapoints for timeseries with count/planned_status in external id.
    # Datapoints for the corresponding good/status timeseries will be returned when querying for count/status timeseries
    # The corresponding timeseries will be uploaded to queue and backfilled
    timeseries_to_query = [
        ts for ts in timeseries_list if "count" in ts.external_id or "planned_status" in ts.external_id
    ]

    clean_uploader_queue = TimeSeriesUploadQueue(
        client,
        post_upload_function=states.post_upload_handler(),
        max_upload_interval=config.extractor.upload_interval,
        max_queue_size=50_000,
        trigger_log_level="INFO",
        thread_name="CDF-Uploader",
    )

    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    futures = []
    with clean_uploader_queue as queue:
        with ThreadPoolExecutor(thread_name_prefix="Data", max_workers=config.extractor.parallelism * 2) as executor:
            if config.backfill.enabled:
                logging.info(f"Starting backfiller. Back-filling for {config.backfill.history_days} days of data")

                for i, batch in enumerate(chunks(timeseries_to_query, 10)):
                    worker = Backfiller(queue, stop_event, ice_cream_api, batch, config, states)
                    futures.append(executor.submit(worker.run))

            if config.frontfill.enabled:
                logging.info("Starting frontfiller...")

                for i, batch in enumerate(chunks(timeseries_to_query, 10)):
                    worker = Streamer(queue, stop_event, ice_cream_api, batch, config, states)
                    futures.append(executor.submit(worker.run))

    for future in as_completed(futures):
        future.result()

    queue.upload()


def main(config_file_path: str = "extractor_config.yaml") -> None:
    """
    Main entrypoint.
    """
    with Extractor(
        name="Ice Cream Factory Datapoints Extractor",
        description="An extractor that ingest datapoints from the Ice Cream Factory API to CDF clean",
        config_class=Config,
        version="1.0",
        config_file_path=config_file_path,
        run_handle=run_extractor,
        metrics=False
    ) as extractor:
        extractor.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run an extractor")
    parser.add_argument(
        "-c",
        "--config",
        dest="config_path",
        type=str,
        help="File containing the configuration for a job",
        nargs="?"
    )

    args = parser.parse_args()
    config_path = args.config_path if args.config_path else "extractor_config.yaml"
    main(config_file_path=config_path)
