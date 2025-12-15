from __future__ import annotations

import io
import json
import threading
from collections import UserDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress

from time import sleep, time
from typing import List, Optional, Tuple
from urllib.parse import quote_plus


import requests

# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT: float = 10.0  # seconds


def _now() -> float:
    """Return current epoch time as float for easy maths."""
    return time()


# ---------------------------------------------------------------------------
# Thread‑safe dict used by ProxyHandler pacing
# ---------------------------------------------------------------------------


class ThreadSafeDict(UserDict):
    """A minimal thread‑safe mapping using an internal RLock."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = threading.RLock()

    def __getitem__(self, key):  # type: ignore[override]
        with self._lock:
            return super().__getitem__(key)

    def __setitem__(self, key, value):  # type: ignore[override]
        with self._lock:
            return super().__setitem__(key, value)

    def __delitem__(self, key):  # type: ignore[override]
        with self._lock:
            return super().__delitem__(key)

    def __contains__(self, key):  # type: ignore[override]
        with self._lock:
            return super().__contains__(key)

    def __iter__(self):  # type: ignore[override]
        with self._lock:
            # iterate over a copy so callers can't mutate during iteration
            return iter(dict(self.data))

    def __len__(self):  # type: ignore[override]
        with self._lock:
            return super().__len__()

    def get(self, key, default=None):  # type: ignore[override]
        with self._lock:
            return self.data.get(key, default)


# ---------------------------------------------------------------------------
# Your existing FastAPI‑based proxy pool
# ---------------------------------------------------------------------------


class ProxyHandler:
    """
    Dispatches HTTP requests through a pool of your FastAPI proxy gateways.

    Each proxy base URL is expected to expose the endpoints:

      - /get_response?url=...
      - /get_response_raw?url=...
      - /file_size?url=...
      - /filepart?url=...&start=...&end=...

    protected with HTTP Basic auth.
    """

    def __init__(
        self,
        proxy_list_file: str,
        proxy_auth: str = "user:password_notdefault",
        port: int = 8000,
        wait_time: float = 0.1,
        timeout: float = DEFAULT_TIMEOUT,
        session: Optional[requests.Session] = None,
    ) -> None:
        try:
            user, pwd = proxy_auth.split(":", 1)
        except ValueError:
            raise ValueError("proxy_auth must be in the form 'user:password'")
        self._auth_tuple: Tuple[str, str] = (user, pwd)
        self._wait_time: float = float(wait_time)
        self._timeout: float = float(timeout)

        self._proxies: List[str] = self._load_proxy_list(proxy_list_file, port)
        if not self._proxies:
            raise ValueError("Proxy list empty")

        self._commit_time: ThreadSafeDict = ThreadSafeDict()
        self._index_lock = threading.Lock()
        self._current_index: int = -1

        # Re‑usable TCP session
        self._session: requests.Session = session or requests.Session()
        self._session.auth = self._auth_tuple
        self._session.headers.update({"Connection": "keep-alive"})

    # ------------------------------------------------------------------ API --

    def get_response(self, url: str):
        """
        Call the proxy `/get_response` endpoint and return the decoded upstream body.

        On success this returns:

        - JSON‑decoded body (from `data`) if available
        - otherwise, raw text (from `text`)

        On any failure or upstream error, returns None.
        """
        url_quoted = quote_plus(url, safe="")
        endpoint = f"get_response?url={url_quoted}"
        result = self._request_through_proxy(endpoint)
        if result is None:
            return None

        resp, idx = result

        with suppress(ValueError, KeyError, TypeError):
            payload = resp.json()
            status_code = int(payload.get("status_code", 0))

            # If the *upstream* target reported a 429, back off this proxy.
            if status_code == 429:
                self._punish_proxy(idx)

            if payload.get("success"):
                # Proxy returns "response" field (text content from upstream)
                response_text = payload.get("response")
                if response_text is not None:
                    # Try to JSON-decode if it looks like JSON
                    try:
                        return json.loads(response_text)
                    except (json.JSONDecodeError, TypeError):
                        return response_text

        return None

    def get(self, url: str) -> Optional[requests.Response]:
        """
        Raw streaming GET via `/get_response_raw`.

        Caller is responsible for inspecting status_code and reading content.
        """
        url_quoted = quote_plus(url, safe="")
        endpoint = f"get_response_raw?url={url_quoted}"
        result = self._request_through_proxy(endpoint)
        return result[0] if result is not None else None

    def filesize(self, url: str) -> Optional[int]:
        """
        Get remote file size via `/file_size` (Content‑Length).

        Returns None if the call failed or the upstream did not respond 200.
        """
        url_quoted = quote_plus(url, safe="")
        endpoint = f"file_size?url={url_quoted}"
        result = self._request_through_proxy(endpoint)
        if not result:
            return None

        resp, _ = result
        if resp.status_code in (200, 206):
            with suppress(ValueError):
                return int(resp.text.strip())
        return None

    def get_filepart(self, url: str, start: int, end: int) -> Optional[requests.Response]:
        """
        Fetch a byte‑range slice of a remote file via `/filepart`.

        Returns a Response (typically 206 or 200) or None.
        """
        url_quoted = quote_plus(url, safe="")
        endpoint = f"filepart?url={url_quoted}&start={start}&end={end}"
        result = self._request_through_proxy(endpoint)
        return result[0] if result is not None else None

    def check(self, raise_exception: bool = False, max_workers: int = 10):
        """
        Concurrent liveness check for all proxies in the pool.

        Returns list of indices that failed the health check.
        """
        failed: List[int] = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(self._check_single, i): i for i in range(len(self._proxies))
            }
            for fut in as_completed(futures):
                idx = futures[fut]
                ok, err = fut.result()
                if not ok:
                    failed.append(idx)
                    print(f"Proxy {self._proxies[idx]} failed: {err}")

        # Remove dead proxies from list
        for idx in sorted(failed, reverse=True):
            del self._proxies[idx]

        if raise_exception and failed:
            raise RuntimeError(f"Proxies {failed} are not working")
        if not self._proxies:
            raise RuntimeError("No proxies available after check")
        return failed

    # ------------------------------------------------------------ internals --

    def _load_proxy_list(self, fp: str, port: int) -> List[str]:
        """
        Read list from file path *fp* and normalise into base HTTP URLs.
        """
        proxies: List[str] = []
        with open(fp, "r", encoding="utf-8") as f:
            for raw in f:
                proxy = raw.strip()
                if not proxy:
                    continue
                if not proxy.startswith("http"):
                    proxy = "http://" + proxy
                # If no explicit port is present, append the configured port.
                if ":" not in proxy.split("//", 1)[-1]:
                    proxy += f":{port}"
                if not proxy.endswith("/"):
                    proxy += "/"
                proxies.append(proxy)
        return proxies

    def _next_proxy_index(self) -> int:
        """Safely increment and return proxy index (round‑robin)."""
        with self._index_lock:
            self._current_index = (self._current_index + 1) % len(self._proxies)
            return self._current_index

    def _wait_until_allowed(self, idx: int) -> None:
        """
        Simple per‑proxy pacing using commit timestamps to avoid hammering
        the same gateway too quickly.
        """
        last_commit = self._commit_time.get(idx, 0.0)
        until = last_commit + self._wait_time
        remaining = until - _now()
        if remaining > 0:
            sleep(remaining)
        self._commit_time[idx] = _now()

    def _request_through_proxy(self, path: str) -> Optional[Tuple[requests.Response, int]]:
        """
        Core request machinery.

        Picks the next proxy, enforces pacing, and returns (response, index)
        on success, or None if the proxy call failed. HTTP 429 from the proxy
        is treated as a rate‑limit signal and punished.
        """
        if not self._proxies:
            return None

        idx = self._next_proxy_index()
        self._wait_until_allowed(idx)
        url = f"{self._proxies[idx]}{path}"

        try:
            resp = self._session.get(url, timeout=self._timeout)
        except Exception as exc:  # broad by design – proxy code favours robustness
            self._punish_proxy(idx)
            print(f"Exception via proxy[{idx}] -> {exc}")
            return None

        if resp.status_code == 429:
            self._punish_proxy(idx)
            print(f"Rate limited by proxy[{idx}] (HTTP 429)")
            return None

        return resp, idx

    def _punish_proxy(self, idx: int) -> None:
        """
        Back off using this proxy for roughly `timeout` seconds.
        """
        self._commit_time[idx] = _now() + self._timeout

    def _check_single(self, idx: int) -> Tuple[bool, Optional[str]]:
        proxy_base = self._proxies[idx]
        try:
            resp = self._session.get(proxy_base, timeout=5)
            if resp.status_code in (200, 206):
                return True, None
            return False, f"status {resp.status_code}"
        except Exception as exc:
            return False, str(exc)

    def __len__(self) -> int:
        """Return the number of proxies available."""
        return len(self._proxies)


class SingleProxyHandler(ProxyHandler):
    """
    Convenience wrapper for handling a single FastAPI proxy URL
    without a proxy list file on disk.
    """

    def __init__(
        self,
        proxy_url: str,
        proxy_auth: str = "user:password_notdefault",
        port: int = 8000,
        wait_time: float = 0.1,
        timeout: float = DEFAULT_TIMEOUT,
        session: Optional[requests.Session] = None,
    ) -> None:
        # Create a pseudo file‑like object containing just this URL
        pseudo_file = io.StringIO(proxy_url)
        super().__init__(
            proxy_list_file=pseudo_file,  # type: ignore[arg-type]
            proxy_auth=proxy_auth,
            port=port,
            wait_time=wait_time,
            timeout=timeout,
            session=session,
        )

    # Override loader to accept the StringIO crafted above.
    def _load_proxy_list(self, fp, port: int):  # type: ignore[override]
        proxy_raw = fp.read().strip()
        if not proxy_raw:
            raise ValueError("Proxy URL is empty for SingleProxyHandler")
        if not proxy_raw.startswith("http"):
            proxy_raw = "http://" + proxy_raw
        if ":" not in proxy_raw.split("//", 1)[-1]:
            proxy_raw += f":{port}"
        if not proxy_raw.endswith("/"):
            proxy_raw += "/"
        return [proxy_raw]


