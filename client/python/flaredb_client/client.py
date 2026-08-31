from __future__ import annotations

import json
import logging
import time
import requests

_LOGGER = logging.getLogger(__name__)

class FlareDBClient:
    """The official FlareDB Python API client wrapper engine client mapping."""
    
    def __init__(self, base_url: str = "http://localhost:8080", retries: int = 3):
        self.base_url = base_url.rstrip('/')
        self.retries = retries
        self.session = requests.Session()

    def connect(self, mode: str = "pico") -> bool:
        """Initializes a targeting hook bound to a 'pico' or 'ds' database architecture cluster."""
        if mode not in ("pico", "ds"):
            raise ValueError("Connection context mode must be flagged as either 'pico' or 'ds'.")
        
        url = f"{self.base_url}/connect"
        payload = {"mode": mode}
        response = self._request_with_retry("POST", url, json=payload)
        return response.status_code == 200

    def create(self, stream_id: str, options: dict | None = None) -> dict:
        """Instantiates a structured record tracking channel partition space."""
        url = f"{self.base_url}/streams"
        payload = {"stream_id": stream_id, "options": options or {}}
        response = self._request_with_retry("POST", url, json=payload)
        return response.json()

    def append(self, stream_id: str, data: dict, durable: bool = True) -> dict:
        """Appends streaming values into a target schema collection utilizing Producer durable elements."""
        url = f"{self.base_url}/streams/{stream_id}/append"
        payload = {"data": data, "require_durable_ack": durable}
        response = self._request_with_retry("POST", url, json=payload)
        return response.json()

    def read(self, stream_id: str, limit: int = 100) -> list[dict]:
        """Queries structured segment ranges historically out of storage segments."""
        url = f"{self.base_url}/streams/{stream_id}"
        params = {"limit": limit}
        response = self._request_with_retry("GET", url, params=params)
        return response.json().get("records", [])

    def subscribe(self, stream_id: str) -> typing.Generator[dict, None, None]:
        """Listens dynamically to realtime engine operations using Server-Sent Events (SSE)."""
        url = f"{self.base_url}/streams/{stream_id}/subscribe"
        
        # Open an long-polling continuous streaming channel
        response = self.session.get(url, stream=True, headers={"Accept": "text/event-stream"})
        response.raise_for_status()

        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                if decoded_line.startswith("data:"):
                    raw_json = decoded_line[5:].strip()
                    yield json.loads(raw_json)

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Private utility executing HTTP mutations accompanied by network retry capabilities."""
        attempt = 0
        while attempt < self.retries:
            try:
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                attempt += 1
                if attempt >= self.retries:
                    raise e
                time.sleep(0.5 * attempt)  # Simple exponential backing algorithm
        raise requests.RequestException("Request engine failed pipeline thresholds.")
