from typing import Dict, Union

from cognite.client.data_classes import Asset
import orjson
from requests import adapters
from requests import Response
from requests import Session


class IceCreamFactoryAPI:
    """Class for Ice Cream Factory API."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.adapter = adapters.HTTPAdapter(max_retries=3)
        self.session = Session()
        self.session.mount("https://", self.adapter)

    def get_response(
        self, headers: Dict[str, str], url_suffix: str, params: Dict[str, Union[str, int, float]] = {}
    ) -> Response:
        """
        Get response from API.

        Args:
            headers: request header
            url_suffix: string to add to base url
            params: query parameters
        """

        response = self.session.get(f"{self.base_url}/{url_suffix}", headers=headers, timeout=40, params=params)
        response.raise_for_status()
        return response

    def get_assets(self):
        """
        Get sites from the Ice Cream API and create a list Assets
        """
        response = self.get_response(headers={}, url_suffix="site/all")

        sites = orjson.loads(response.content)
        
        # remove labels from list of sites
        sites = [{k:v for k,v in site.items() if k != "labels"} for site in sites]
        assets = [Asset(**site) for site in sites]

        return assets
