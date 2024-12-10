import os.path
from urllib.parse import urljoin

import pendulum
import requests
from airflow.hooks.base import BaseHook
from requests.auth import HTTPBasicAuth


def get_dmptool_api_creds(aws_conn_id: str) -> tuple[str, str]:
    """Get the AWS access key id and secret access key from the aws_conn_id airflow connection.

    :return: access key id and secret access key
    """

    conn = BaseHook.get_connection(aws_conn_id)
    client_id = conn.login
    client_secret = conn.password

    if client_id is None:
        raise ValueError(f"Airflow Connection: {aws_conn_id} login is None")

    if client_secret is None:
        raise ValueError(f"Airflow Connection: {aws_conn_id} password is None")

    return client_id, client_secret


def get_latest_files(data) -> tuple[list[tuple[str, str]], pendulum.DateTime | None]:
    # Extract the files and URLs from the dictionary
    files = data.get("DMPMetadataFiles", {})
    if not len(files):
        return [], None

    # Parse the dates from the filenames and group by date
    file_groups = {}
    for filename, url in files.items():
        # Extract the date portion (assumes format 'coki-dmps_YYYY-MM-DD_*.jsonl.gz')
        date_str = filename.split("_")[1]
        file_date = pendulum.parse(date_str, exact=True)
        file_date = pendulum.DateTime(file_date.year, file_date.month, file_date.day)
        if file_date not in file_groups:
            file_groups[file_date] = []
        file_groups[file_date].append((filename, url))

    # Find the latest date
    latest_date = max(file_groups.keys())

    # Return the list of files for the latest date
    return file_groups[latest_date], latest_date


class DMPToolAPI:
    def __init__(self, *, env: str, client_id: str, client_secret: str):
        self.env = env
        self.base_url = f"https://api.dmphub.uc3{self.env}.cdlib.net"
        self.client_id = client_id
        self.client_secret = client_secret

    def fetch_dmps(self) -> tuple[list[tuple[str, str]], pendulum.DateTime]:
        # Fetch DMP download URLs
        oauth_token = self._oauth_token()
        url = urljoin(self.base_url, "dmps/downloads")
        headers = {"Authorization": f"Bearer {oauth_token}", "Content-Type": "application/json"}
        response = requests.get(url, headers=headers)

        # Check response has valid status
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(
                f"DMPToolAPI.fetch_dmps: error fetching DMP download files. Status code: {response.status_code}. Reason: {response.reason}"
            )

        # Return files for latest release
        json_data = response.json()
        latest_files, release_date = get_latest_files(json_data)

        return latest_files, release_date

    def upload_match(self, file_path: str):
        file_name = os.path.basename(file_path)
        presigned_url = self._make_presigned_upload_url(file_name)

        # Upload file to presigned URL
        with open(file_path, "rb") as file:
            response = requests.put(presigned_url, data=file)
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(
                f"DMPToolAPI.upload_match: error uploading file {file_path}. Status code: {response.status_code}. Reason: {response.reason}"
            )

    def _make_presigned_upload_url(self, file_name: str):
        # Generate presigned URL
        oauth_token = self._oauth_token()
        url = urljoin(self.base_url, "dmps/uploads")
        headers = {"Authorization": f"Bearer {oauth_token}", "Content-Type": "application/json"}
        data = {"fileName": file_name}
        response = requests.put(url, headers=headers, json=data)

        # Check response status
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(
                f"DMPToolAPI.upload_match: error getting presigned URL for {file_name}. Status code: {response.status_code}. Reason: {response.reason}"
            )

        # Get presigned URL
        json_data = response.json()
        presigned_url = json_data.get("UploadDestination", {}).get(file_name)
        if presigned_url is None:
            raise ValueError(f"DMPToolAPI.upload_match: no presigned URL returned for {file_name}")

        return presigned_url

    def _oauth_token(self):
        url = urljoin(f"https://auth.dmphub.uc3{self.env}.cdlib.net", "oauth2/token")
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "scope": f"https://auth.dmphub.uc3{self.env}.cdlib.net/{self.env}.data-transfer",
        }
        response = requests.post(
            url, headers=headers, data=data, auth=HTTPBasicAuth(self.client_id, self.client_secret)
        )
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(
                f"DMPToolAPI.oauth_token: error fetching OAuth Token. Status code: {response.status_code}. Reason: {response.reason}"
            )
        json_data = response.json()
        if "access_token" not in json_data:
            raise ValueError(f"DMPToolAPI.oauth_token: access_token not returned in response")
        return json_data["access_token"]
