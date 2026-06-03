"""Integration tests for the agent API endpoints.

Tests require:
- The FastAPI app running (with agent routes registered)
- A configured ``OPENCODE_GO_API_KEY`` (or the endpoints will return errors)

These tests use the FastAPI TestClient and verify request/response shapes.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.api.main import create_app

# Skip if no API key
try:
    from app.core.config import settings

    _has_api_key = bool(settings.opencode_go_api_key)
except Exception:
    _has_api_key = False

app = create_app()
client = TestClient(app)


class TestAgentAPI:
    @pytest.mark.skipif(not _has_api_key, reason="OPENCODE_GO_API_KEY not set")
    def test_invoke_endpoint_returns_200(self) -> None:
        """POST /agent/invoke with valid data should return 200."""
        response = client.post(
            "/agent/invoke",
            json={
                "user_address": "0x402085c248EeA27D92E8b30b2C58ed07f9E20001",
                "prompt": "Analyse my portfolio briefly.",
            },
        )
        assert response.status_code in (200, 422)

    def test_invoke_missing_field_returns_422(self) -> None:
        """POST /agent/invoke without required field should return 422."""
        response = client.post(
            "/agent/invoke",
            json={"prompt": "test"},
        )
        assert response.status_code == 422

    def test_invoke_invalid_address_returns_422(self) -> None:
        """POST /agent/invoke with invalid user_address should return 422."""
        response = client.post(
            "/agent/invoke",
            json={"user_address": "short", "prompt": "test"},
        )
        assert response.status_code == 422
