"""SYNQ REST API client with OAuth2 authentication.

Single implementation of SYNQ auth and request handling.
Credentials come from SYNQ_CLIENT_ID and SYNQ_CLIENT_SECRET env vars.
"""

from __future__ import annotations

import os
from typing import Any, Optional

import requests
from rich.console import Console

from mc_to_synq.config import AppConfig

console = Console()


class SynqClient:
    """Authenticated client for the SYNQ REST API.

    Handles OAuth2 token acquisition, automatic refresh on 401,
    and all HTTP methods against the SYNQ API.
    """

    def __init__(self, config: AppConfig) -> None:
        self.base_url = config.synq.base_url.rstrip("/")
        self.oauth_url = config.synq.oauth_url
        self.integration_id = config.synq.integration_id
        self.verify_ssl = config.network.verify_ssl
        self.timeout = config.network.timeout
        self._access_token: Optional[str] = None

        if not self.verify_ssl:
            requests.packages.urllib3.disable_warnings(  # type: ignore[attr-defined]
                requests.packages.urllib3.exceptions.InsecureRequestWarning  # type: ignore[attr-defined]
            )

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _resolve_credentials(self) -> tuple[str, str]:
        client_id = os.environ.get("SYNQ_CLIENT_ID", "")
        client_secret = os.environ.get("SYNQ_CLIENT_SECRET", "")
        if not client_id or not client_secret:
            raise EnvironmentError(
                "SYNQ credentials not found. Set SYNQ_CLIENT_ID and SYNQ_CLIENT_SECRET "
                "environment variables.\n"
                "Create credentials at: SYNQ Settings -> API -> Add client\n"
                "Required scopes: Edit SQL Tests, Edit Automatic Monitors, Edit Custom Monitors"
            )
        return client_id, client_secret

    def authenticate(self) -> str:
        """Fetch an OAuth2 access token from SYNQ."""
        client_id, client_secret = self._resolve_credentials()

        response = requests.post(
            self.oauth_url,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            },
            verify=self.verify_ssl,
            timeout=self.timeout,
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"SYNQ auth failed (HTTP {response.status_code}): {response.text[:300]}"
            )

        data = response.json()
        self._access_token = data["access_token"]
        return self._access_token

    def _ensure_token(self) -> str:
        if not self._access_token:
            self.authenticate()
        return self._access_token  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._ensure_token()}",
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> requests.Response:
        """Execute an authenticated request, retrying once on 401."""
        url = f"{self.base_url}{path}"

        response = requests.request(
            method,
            url,
            headers=self._headers(),
            json=data,
            params=params,
            verify=self.verify_ssl,
            timeout=self.timeout,
        )

        if response.status_code == 401:
            self.authenticate()
            response = requests.request(
                method,
                url,
                headers=self._headers(),
                json=data,
                params=params,
                verify=self.verify_ssl,
                timeout=self.timeout,
            )

        return response

    def get(
        self, path: str, params: Optional[dict[str, Any]] = None
    ) -> requests.Response:
        return self._request("GET", path, params=params)

    def post(
        self, path: str, data: Optional[dict[str, Any]] = None
    ) -> requests.Response:
        return self._request("POST", path, data=data)

    def delete(
        self, path: str, params: Optional[dict[str, Any]] = None
    ) -> requests.Response:
        return self._request("DELETE", path, params=params)

    def test_connection(self) -> bool:
        """Verify SYNQ API connectivity by listing monitors."""
        response = self.get("/api/monitors/custom-monitors/v1")
        return response.status_code == 200
