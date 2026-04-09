from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx


class ClientConfigError(RuntimeError):
    """Raised when a required client configuration value is missing."""


class ExternalServiceError(RuntimeError):
    """Raised when an upstream provider request fails."""


@dataclass(slots=True)
class ConnectionCheck:
    service: str
    ok: bool
    detail: str
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "ok": self.ok,
            "state": "healthy" if self.ok else "down",
            "detail": self.detail,
            "meta": self.meta,
        }


class BaseAPIClient:
    service_name = "external"

    def __init__(
        self,
        base_url: str | None,
        *,
        headers: dict[str, str] | None = None,
        timeout: float = 15.0,
    ) -> None:
        self.timeout = timeout
        self.base_url = ""
        self.headers: dict[str, str] = {}
        self._set_connection(base_url, headers=headers)

    def _set_connection(
        self,
        base_url: str | None,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.base_url = (base_url or "").strip().rstrip("/")
        self.headers = {"Accept": "application/json"}
        if headers:
            self.headers.update(headers)

    @property
    def configured(self) -> bool:
        return bool(self.base_url)

    def _require_base_url(self) -> None:
        if not self.base_url:
            raise ClientConfigError(f"{self.service_name} base URL is not configured.")

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Any:
        self._require_base_url()
        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)

        try:
            request_timeout = self.timeout if timeout is None else timeout
            async with httpx.AsyncClient(timeout=request_timeout) as client:
                response = await client.request(
                    method=method,
                    url=f"{self.base_url}{path}",
                    params=params,
                    json=json_body,
                    headers=request_headers,
                )
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            message = exc.response.text.strip() or exc.response.reason_phrase
            raise ExternalServiceError(
                f"{self.service_name} returned HTTP {status}: {message[:500]}"
            ) from exc
        except httpx.HTTPError as exc:
            detail = str(exc).strip() or exc.__class__.__name__
            raise ExternalServiceError(f"{self.service_name} request failed: {detail}") from exc

        if not response.content:
            return {}

        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            return response.json()

        return response.text
