"""Monte Carlo GraphQL API client.

Single implementation of MC authentication and query execution.
Reads credentials from ~/.mcd/profiles.ini or environment variables.
"""

from __future__ import annotations

import configparser
import os
from pathlib import Path
from typing import Any, Optional

import requests
from rich.console import Console

from mc_to_synq.config import AppConfig

console = Console()


class MonteCarloClient:
    """Authenticated client for the Monte Carlo GraphQL API."""

    def __init__(self, config: AppConfig) -> None:
        self.api_url = config.monte_carlo.api_url
        self.verify_ssl = config.network.verify_ssl
        self.timeout = config.network.timeout
        self._profile = config.monte_carlo.credentials_profile
        self._mcd_id: Optional[str] = None
        self._mcd_token: Optional[str] = None

        if not self.verify_ssl:
            requests.packages.urllib3.disable_warnings(  # type: ignore[attr-defined]
                requests.packages.urllib3.exceptions.InsecureRequestWarning  # type: ignore[attr-defined]
            )

    # ------------------------------------------------------------------
    # Credential resolution
    # ------------------------------------------------------------------

    def _load_credentials(self) -> tuple[str, str]:
        """Resolve MC credentials from env vars or profiles.ini."""
        # Env vars take priority
        env_id = os.environ.get("MC_API_ID")
        env_token = os.environ.get("MC_API_TOKEN")
        if env_id and env_token:
            return env_id, env_token

        # Fall back to ~/.mcd/profiles.ini
        profiles_path = Path.home() / ".mcd" / "profiles.ini"
        if not profiles_path.exists():
            raise FileNotFoundError(
                f"MC credentials not found. Set MC_API_ID and MC_API_TOKEN env vars, "
                f"or create {profiles_path} with:\n"
                f"  [{self._profile}]\n"
                f"  mcd_id = your_id\n"
                f"  mcd_token = your_token"
            )

        config = configparser.ConfigParser()
        config.read(profiles_path)

        if self._profile not in config:
            raise ValueError(
                f"Profile '{self._profile}' not found in {profiles_path}. "
                f"Available profiles: {list(config.sections())}"
            )

        mcd_id = config[self._profile].get("mcd_id", "")
        mcd_token = config[self._profile].get("mcd_token", "")

        if not mcd_id or not mcd_token:
            raise ValueError(
                f"Profile '{self._profile}' in {profiles_path} is missing "
                f"mcd_id or mcd_token."
            )

        return mcd_id, mcd_token

    def _ensure_credentials(self) -> tuple[str, str]:
        if self._mcd_id and self._mcd_token:
            return self._mcd_id, self._mcd_token
        self._mcd_id, self._mcd_token = self._load_credentials()
        return self._mcd_id, self._mcd_token

    # ------------------------------------------------------------------
    # GraphQL execution
    # ------------------------------------------------------------------

    def query(
        self,
        gql: str,
        variables: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query against Monte Carlo.

        Returns the full response dict (including 'data' and optionally 'errors').
        Raises on HTTP-level failures.
        """
        mcd_id, mcd_token = self._ensure_credentials()

        headers = {
            "Content-Type": "application/json",
            "x-mcd-id": mcd_id,
            "x-mcd-token": mcd_token,
        }

        payload: dict[str, Any] = {"query": gql}
        if variables:
            payload["variables"] = variables

        response = requests.post(
            self.api_url,
            headers=headers,
            json=payload,
            verify=self.verify_ssl,
            timeout=self.timeout,
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"MC API returned HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )

        data = response.json()

        if "errors" in data:
            for err in data["errors"][:3]:
                console.print(
                    f"  [yellow]GraphQL warning:[/yellow] {err.get('message', 'unknown')}"
                )

        return data

    def test_connection(self) -> str:
        """Verify connectivity. Returns the authenticated user's email."""
        result = self.query("{ getUser { email firstName lastName } }")
        user = result.get("data", {}).get("getUser")
        if not user:
            raise RuntimeError("MC API returned no user data. Check credentials.")
        name = f"{user.get('firstName', '')} {user.get('lastName', '')}".strip()
        email = user.get("email", "unknown")
        return f"{name} ({email})" if name else email
