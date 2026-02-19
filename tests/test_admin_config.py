GET_URL = "/api/admin/config/min-search-version"
PUT_URL = "/api/admin/config/min-search-version"


# ---------------------------------------------------------------------------
# Test 1: GET returns fallback value when no DB row
# ---------------------------------------------------------------------------


async def test_get_min_search_version_default(admin_client):
    """GET /api/admin/config/min-search-version returns fallback value."""
    resp = await admin_client.client.get(GET_URL)

    assert resp.status_code == 200
    data = resp.json()
    assert "min_search_app_version" in data
    # Should return a valid version string (either DB seed or settings fallback)
    assert data["min_search_app_version"]


# ---------------------------------------------------------------------------
# Test 2: PUT updates, GET returns new value
# ---------------------------------------------------------------------------


async def test_put_min_search_version(admin_client):
    """PUT updates version, subsequent GET returns the new value."""
    put_resp = await admin_client.client.put(
        PUT_URL,
        json={"version": "2.5.0"},
    )
    assert put_resp.status_code == 200
    assert put_resp.json()["min_search_app_version"] == "2.5.0"

    get_resp = await admin_client.client.get(GET_URL)
    assert get_resp.status_code == 200
    assert get_resp.json()["min_search_app_version"] == "2.5.0"


# ---------------------------------------------------------------------------
# Test 3: PUT with invalid version format -> 422
# ---------------------------------------------------------------------------


async def test_put_invalid_version_format(admin_client):
    """PUT with non-semver version -> 422 validation error."""
    resp = await admin_client.client.put(
        PUT_URL,
        json={"version": "1.0"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 4: PUT unauthenticated -> 401
# ---------------------------------------------------------------------------


async def test_put_unauthenticated(app_client):
    """PUT without admin session -> 401."""
    resp = await app_client.put(
        PUT_URL,
        json={"version": "2.0.0"},
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Test 5: GET unauthenticated -> 401
# ---------------------------------------------------------------------------


async def test_get_unauthenticated(app_client):
    """GET without admin session -> 401."""
    resp = await app_client.get(GET_URL)
    assert resp.status_code == 401
