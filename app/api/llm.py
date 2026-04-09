from __future__ import annotations

import json
from typing import Any

import httpx
from litellm import acompletion

from app.api.base import ClientConfigError, ConnectionCheck, ExternalServiceError
from app.core.settings import LLMProviderSettings, Settings


class LLMClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _current_settings(self) -> Settings:
        if hasattr(self.settings, "snapshot"):
            return self.settings.snapshot()
        return self.settings

    async def test_provider(self, provider: LLMProviderSettings) -> ConnectionCheck:
        settings = self._current_settings()
        return await self._test_provider_connection(settings, provider, failover_count=0)

    async def list_ollama_models(self, provider: LLMProviderSettings) -> list[str]:
        settings = self._current_settings()
        return await self._list_ollama_models(settings, provider)

    async def test_connection(self) -> ConnectionCheck:
        settings = self._current_settings()
        providers = settings.active_llm_providers
        if not providers:
            return ConnectionCheck(
                service="LLM",
                ok=False,
                detail="No LLM providers are configured.",
                meta={"providers": []},
            )

        errors: list[str] = []
        try:
            for provider in providers:
                try:
                    return await self._test_provider_connection(
                        settings,
                        provider,
                        failover_count=max(0, len(providers) - 1),
                    )
                except (ClientConfigError, ExternalServiceError) as exc:
                    errors.append(f"{provider.name}: {exc}")
        except Exception as exc:  # pragma: no cover - defensive top-level catch
            return ConnectionCheck(
                service="LLM",
                ok=False,
                detail=str(exc),
                meta={"providers": [provider.provider for provider in providers]},
            )
        return ConnectionCheck(
            service="LLM",
            ok=False,
            detail=f"All configured LLM providers failed. {' | '.join(errors[:3])}",
            meta={"providers": [provider.provider for provider in providers]},
        )

    async def generate_text(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
        timeout_seconds: int | None = None,
    ) -> str:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        return await self.generate_messages(
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            timeout_seconds=timeout_seconds,
        )

    async def generate_messages(
        self,
        *,
        messages: list[dict[str, Any]],
        max_tokens: int | None = None,
        temperature: float | None = None,
        timeout_seconds: int | None = None,
    ) -> str:
        settings = self._current_settings()
        providers = settings.active_llm_providers
        if not providers:
            raise ClientConfigError("No LLM providers are configured.")

        errors: list[str] = []
        for provider in providers:
            try:
                return await self._generate_messages_with_provider(
                    settings=settings,
                    provider=provider,
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    timeout_seconds=timeout_seconds,
                )
            except (ClientConfigError, ExternalServiceError) as exc:
                errors.append(f"{provider.name}: {exc}")

        raise ExternalServiceError(f"LLM request failed across all providers. {' | '.join(errors[:3])}")

    async def generate_json(
        self,
        *,
        messages: list[dict[str, Any]],
        max_tokens: int | None = None,
        temperature: float | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        raw_text = await self.generate_messages(
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            timeout_seconds=timeout_seconds,
        )
        return self._extract_json_object(raw_text)

    async def _generate_messages_with_provider(
        self,
        *,
        settings: Settings,
        provider: LLMProviderSettings,
        messages: list[dict[str, Any]],
        max_tokens: int | None,
        temperature: float | None,
        timeout_seconds: int | None,
    ) -> str:
        self._validate_provider_config(settings, provider)
        kwargs = self._build_completion_kwargs(
            settings=settings,
            provider=provider,
            max_tokens=max_tokens,
            temperature=temperature,
            timeout_seconds=timeout_seconds,
        )
        try:
            response = await acompletion(messages=messages, **kwargs)
        except Exception as exc:  # pragma: no cover - upstream exceptions vary by provider
            raise ExternalServiceError(f"LLM request failed via {provider.name}: {exc}") from exc
        return self._extract_text(response)

    def _validate_provider_config(self, settings: Settings, provider: LLMProviderSettings) -> None:
        provider_name = provider.provider.lower()
        if provider_name not in {"ollama", "openai", "anthropic"}:
            raise ClientConfigError(f"Unsupported LLM provider '{provider.provider}'.")
        if not provider.model:
            raise ClientConfigError("LLM_MODEL is not configured.")

        if provider_name == "ollama" and not self._provider_api_base(settings, provider):
            raise ClientConfigError("OLLAMA_API_BASE is required when provider=ollama.")
        if provider_name == "openai" and not self._provider_api_key(settings, provider):
            raise ClientConfigError("OPENAI_API_KEY is required when provider=openai.")
        if provider_name == "anthropic" and not self._provider_api_key(settings, provider):
            raise ClientConfigError("ANTHROPIC_API_KEY is required when provider=anthropic.")

    def _build_completion_kwargs(
        self,
        *,
        settings: Settings | None = None,
        provider: LLMProviderSettings | None = None,
        max_tokens: int | None,
        temperature: float | None,
        timeout_seconds: int | None,
    ) -> dict[str, Any]:
        if settings is None:
            settings = self._current_settings()
        if provider is None:
            provider = settings.primary_llm_provider or settings.legacy_llm_provider
        if provider is None:
            raise ClientConfigError("No LLM providers are configured.")
        provider_name = provider.provider.lower()
        kwargs: dict[str, Any] = {
            "model": self._resolve_model_name(provider),
            "timeout": timeout_seconds or self._effective_timeout_seconds(settings, provider),
            "max_tokens": max_tokens or settings.llm_max_output_tokens,
            "temperature": settings.llm_temperature if temperature is None else temperature,
        }

        api_base = self._provider_api_base(settings, provider)
        api_key = self._provider_api_key(settings, provider)
        if provider_name == "ollama":
            kwargs["api_base"] = api_base
        elif provider_name in {"openai", "anthropic"}:
            kwargs["api_key"] = api_key
            if api_base:
                kwargs["api_base"] = api_base

        return kwargs

    def _resolve_model_name(self, provider: LLMProviderSettings) -> str:
        provider_name = provider.provider.lower()
        model = provider.model.strip()
        if provider_name == "ollama" and "/" not in model:
            return f"ollama/{model}"
        return model

    async def _ping_ollama(self, settings: Settings, provider: LLMProviderSettings) -> dict[str, Any]:
        api_base = self._provider_api_base(settings, provider)
        if not api_base:
            raise ClientConfigError("OLLAMA_API_BASE is required when provider=ollama.")
        async with httpx.AsyncClient(
            timeout=min(self._effective_timeout_seconds(settings, provider), 8),
            base_url=api_base.rstrip("/"),
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

    async def _test_provider_connection(
        self,
        settings: Settings,
        provider: LLMProviderSettings,
        *,
        failover_count: int,
    ) -> ConnectionCheck:
        self._validate_provider_config(settings, provider)
        if provider.provider.lower() == "ollama":
            models = await self._list_ollama_models(settings, provider)
            return ConnectionCheck(
                service="LLM",
                ok=True,
                detail=f"{provider.name} is reachable for model {provider.model}.",
                meta={
                    "provider": provider.provider,
                    "provider_name": provider.name,
                    "model": provider.model,
                    "priority": provider.priority,
                    "models": models[:5],
                    "failover_count": failover_count,
                },
            )

        reply = await self._generate_messages_with_provider(
            settings=settings,
            provider=provider,
            messages=[
                {"role": "system", "content": "You are a health check. Reply with OK."},
                {"role": "user", "content": "Respond with OK."},
            ],
            max_tokens=8,
            temperature=0,
            timeout_seconds=min(self._effective_timeout_seconds(settings, provider), 8),
        )
        return ConnectionCheck(
            service="LLM",
            ok="OK" in reply.upper(),
            detail=f"{provider.name} responded for model {provider.model}.",
            meta={
                "provider": provider.provider,
                "provider_name": provider.name,
                "model": provider.model,
                "priority": provider.priority,
                "failover_count": failover_count,
            },
        )

    async def _list_ollama_models(self, settings: Settings, provider: LLMProviderSettings) -> list[str]:
        if provider.provider.lower() != "ollama":
            raise ClientConfigError("Model listing is only available for Ollama providers.")

        api_base = self._provider_api_base(settings, provider)
        if not api_base:
            raise ClientConfigError("OLLAMA_API_BASE is required when provider=ollama.")

        payload = await self._ping_ollama(settings, provider)
        models: list[str] = []
        seen: set[str] = set()
        for item in payload.get("models", []):
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            lowered = name.lower()
            if lowered in seen:
                continue
            models.append(name)
            seen.add(lowered)
        return models

    @staticmethod
    def _provider_api_key(settings: Settings, provider: LLMProviderSettings) -> str | None:
        if provider.api_key:
            return provider.api_key
        provider_name = provider.provider.lower()
        if provider_name == "openai":
            return settings.openai_api_key
        if provider_name == "anthropic":
            return settings.anthropic_api_key
        return None

    @staticmethod
    def _provider_api_base(settings: Settings, provider: LLMProviderSettings) -> str | None:
        if provider.api_base:
            return provider.api_base
        provider_name = provider.provider.lower()
        if provider_name == "ollama":
            return settings.ollama_api_base
        if provider_name == "openai":
            return settings.openai_api_base
        if provider_name == "anthropic":
            return settings.anthropic_api_base
        return None

    @staticmethod
    def _effective_timeout_seconds(settings: Settings, provider: LLMProviderSettings) -> int:
        return settings.resolve_llm_timeout(provider.provider, provider.timeout_seconds)

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
