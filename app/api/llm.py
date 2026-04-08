from __future__ import annotations

import json
from typing import Any

import httpx
from litellm import acompletion

from app.api.base import ClientConfigError, ConnectionCheck, ExternalServiceError
from app.core.settings import Settings


class LLMClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def test_connection(self) -> ConnectionCheck:
        provider = self.settings.llm_provider.lower()
        model = self.settings.llm_model

        try:
            self._validate_config()
            if provider == "ollama":
                payload = await self._ping_ollama()
                models = [item.get("name") for item in payload.get("models", [])[:5]]
                return ConnectionCheck(
                    service="LLM",
                    ok=True,
                    detail=f"Ollama is reachable for model {model}.",
                    meta={"provider": provider, "model": model, "models": models},
                )

            reply = await self.generate_text(
                system_prompt="You are a health check. Reply with OK.",
                user_prompt="Respond with OK.",
                max_tokens=8,
                temperature=0,
            )
            return ConnectionCheck(
                service="LLM",
                ok="OK" in reply.upper(),
                detail=f"{provider} responded for model {model}.",
                meta={"provider": provider, "model": model},
            )
        except (ClientConfigError, ExternalServiceError) as exc:
            return ConnectionCheck(
                service="LLM",
                ok=False,
                detail=str(exc),
                meta={"provider": provider, "model": model},
            )

    async def generate_text(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        return await self.generate_messages(
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )

    async def generate_messages(
        self,
        *,
        messages: list[dict[str, Any]],
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        self._validate_config()

        kwargs = self._build_completion_kwargs(
            max_tokens=max_tokens,
            temperature=temperature,
        )

        try:
            response = await acompletion(messages=messages, **kwargs)
        except Exception as exc:  # pragma: no cover - upstream exceptions vary by provider
            raise ExternalServiceError(f"LLM request failed: {exc}") from exc

        return self._extract_text(response)

    async def generate_json(
        self,
        *,
        messages: list[dict[str, Any]],
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> dict[str, Any]:
        raw_text = await self.generate_messages(
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return self._extract_json_object(raw_text)

    def _validate_config(self) -> None:
        provider = self.settings.llm_provider.lower()
        if not self.settings.llm_model:
            raise ClientConfigError("LLM_MODEL is not configured.")

        if provider == "ollama" and not self.settings.ollama_api_base:
            raise ClientConfigError("OLLAMA_API_BASE is required when LLM_PROVIDER=ollama.")
        if provider == "openai" and not self.settings.openai_api_key:
            raise ClientConfigError("OPENAI_API_KEY is required when LLM_PROVIDER=openai.")
        if provider == "anthropic" and not self.settings.anthropic_api_key:
            raise ClientConfigError("ANTHROPIC_API_KEY is required when LLM_PROVIDER=anthropic.")

    def _build_completion_kwargs(
        self,
        *,
        max_tokens: int | None,
        temperature: float | None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "model": self.settings.llm_model,
            "timeout": self.settings.llm_timeout_seconds,
            "max_tokens": max_tokens or self.settings.llm_max_output_tokens,
            "temperature": self.settings.llm_temperature if temperature is None else temperature,
        }

        provider = self.settings.llm_provider.lower()
        if provider == "ollama":
            kwargs["api_base"] = self.settings.ollama_api_base
        elif provider == "openai":
            kwargs["api_key"] = self.settings.openai_api_key
            if self.settings.openai_api_base:
                kwargs["api_base"] = self.settings.openai_api_base
        elif provider == "anthropic":
            kwargs["api_key"] = self.settings.anthropic_api_key
            if self.settings.anthropic_api_base:
                kwargs["api_base"] = self.settings.anthropic_api_base

        return kwargs

    async def _ping_ollama(self) -> dict[str, Any]:
        async with httpx.AsyncClient(
            timeout=self.settings.llm_timeout_seconds,
            base_url=self.settings.ollama_api_base.rstrip("/"),
        ) as client:
            try:
                response = await client.get("/api/tags")
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise ExternalServiceError(
                    f"Ollama returned HTTP {exc.response.status_code}: {exc.response.text[:500]}"
                ) from exc
            except httpx.HTTPError as exc:
                raise ExternalServiceError(f"Ollama request failed: {exc}") from exc

        return response.json()

    @staticmethod
    def _extract_text(response: Any) -> str:
        choices = getattr(response, "choices", None) or response.get("choices", [])
        if not choices:
            return ""

        message = choices[0].get("message") if isinstance(choices[0], dict) else choices[0].message
        content = message.get("content") if isinstance(message, dict) else message.content

        if isinstance(content, str):
            return content.strip()

        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict):
                    text = item.get("text")
                    if text:
                        parts.append(str(text))
                elif hasattr(item, "text"):
                    parts.append(str(item.text))
            return "\n".join(parts).strip()

        return str(content).strip()

    @staticmethod
    def _extract_json_object(raw_text: str) -> dict[str, Any]:
        candidate = raw_text.strip()
        if not candidate:
            raise ExternalServiceError("LLM returned an empty response.")

        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

        start = candidate.find("{")
        end = candidate.rfind("}")
        if start == -1 or end == -1 or start >= end:
            raise ExternalServiceError("LLM response did not contain a valid JSON object.")

        try:
            return json.loads(candidate[start : end + 1])
        except json.JSONDecodeError as exc:
            raise ExternalServiceError("LLM response contained invalid JSON.") from exc
