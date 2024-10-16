import os
from pprint import pprint

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials

from code.handler import handle


def main() -> None:
    credentials = OAuthClientCredentials(
        token_url="https://login.microsoftonline.com/16e3985b-ebe8-4e24-9da4-933e21a9fc81/oauth2/v2.0/token",
        client_id="6770a1e4-852c-4653-9d57-740fe45452c6",
        client_secret=os.environ["IDP_CLIENT_SECRET"],
        scopes=['https://westeurope-1.cognitedata.com/.default'],
    )

    client = CogniteClient(
        config=ClientConfig(
            client_name="CDF-Toolkit:0.3.2",
            project="cdf-bootcamp-70-test",
            base_url="https://westeurope-1.cognitedata.com",
            credentials=credentials,
        )
    )

    print("icapi_assets_extractor LOGS:")
    response = handle(
        client=client,
        data={},
    )

    print("icapi_assets_extractor RESPONSE:")
    pprint(response)


if __name__ == "__main__":
    main()
