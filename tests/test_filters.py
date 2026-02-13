FILTERS_URL = "/api/v1/filters"
REGISTER_URL = "/api/v1/auth/register"

_TEST_PASSWORD = "securePass1"

_DEFAULT_FILTERS = {
    "min_price": 20.0,
    "start_time": "06:30",
    "working_time": 24,
    "working_days": ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"],
}

_VALID_UPDATE = {
    "min_price": 25.0,
    "start_time": "07:00",
    "working_time": 12,
    "working_days": ["MON", "TUE", "WED", "THU", "FRI"],
}


async def _register_and_get_tokens(app_client, email="filters@example.com"):
    """Helper: register a user via API and return response data with tokens."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


def _auth_header(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


# --- Test Strategy 1: GET /filters without JWT → 401 ---


async def test_get_filters_without_jwt_returns_401(app_client):
    """GET /filters without Authorization header → 401."""
    response = await app_client.get(FILTERS_URL)

    assert response.status_code == 401


# --- Test Strategy 2: GET /filters with JWT → 200 + defaults ---


async def test_get_filters_returns_defaults_for_new_user(app_client):
    """GET /filters with JWT → 200 with default filter values."""
    reg = await _register_and_get_tokens(app_client, email="defaults@example.com")

    response = await app_client.get(
        FILTERS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == _DEFAULT_FILTERS


# --- Test Strategy 3: PUT /filters with valid data → 200 ---


async def test_put_filters_valid_data_returns_200(app_client):
    """PUT /filters with valid data → 200 {"ok": true}."""
    reg = await _register_and_get_tokens(app_client, email="valid@example.com")

    response = await app_client.put(
        FILTERS_URL,
        json=_VALID_UPDATE,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- Test Strategy 4: PUT /filters twice → idempotent ---


async def test_put_filters_twice_is_idempotent(app_client):
    """PUT /filters twice with same data → both return 200, GET returns same values."""
    reg = await _register_and_get_tokens(app_client, email="idempotent@example.com")
    headers = _auth_header(reg["access_token"])

    resp1 = await app_client.put(FILTERS_URL, json=_VALID_UPDATE, headers=headers)
    assert resp1.status_code == 200

    resp2 = await app_client.put(FILTERS_URL, json=_VALID_UPDATE, headers=headers)
    assert resp2.status_code == 200

    get_resp = await app_client.get(FILTERS_URL, headers=headers)
    assert get_resp.json() == _VALID_UPDATE


# --- Test Strategy 5: GET returns updated values after PUT ---


async def test_get_filters_returns_updated_values_after_put(app_client):
    """PUT /filters → GET /filters returns updated values."""
    reg = await _register_and_get_tokens(app_client, email="updated@example.com")
    headers = _auth_header(reg["access_token"])

    await app_client.put(FILTERS_URL, json=_VALID_UPDATE, headers=headers)

    response = await app_client.get(FILTERS_URL, headers=headers)

    assert response.status_code == 200
    data = response.json()
    assert data["min_price"] == 25.0
    assert data["start_time"] == "07:00"
    assert data["working_time"] == 12
    assert data["working_days"] == ["MON", "TUE", "WED", "THU", "FRI"]


# --- Validation: PUT /filters with min_price < 10 → 422 ---


async def test_put_filters_min_price_below_minimum_returns_422(app_client):
    """PUT /filters with min_price < 10 → 422."""
    reg = await _register_and_get_tokens(app_client, email="lowprice@example.com")

    response = await app_client.put(
        FILTERS_URL,
        json={**_VALID_UPDATE, "min_price": 5.0},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 422


# --- Validation: PUT /filters with empty working_days → 422 ---


async def test_put_filters_empty_working_days_returns_422(app_client):
    """PUT /filters with empty working_days → 422."""
    reg = await _register_and_get_tokens(app_client, email="nodays@example.com")

    response = await app_client.put(
        FILTERS_URL,
        json={**_VALID_UPDATE, "working_days": []},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 422


# --- Validation: PUT /filters with working_time outside 1-24 → 422 ---


async def test_put_filters_working_time_below_minimum_returns_422(app_client):
    """PUT /filters with working_time < 1 → 422."""
    reg = await _register_and_get_tokens(app_client, email="lowtime@example.com")

    response = await app_client.put(
        FILTERS_URL,
        json={**_VALID_UPDATE, "working_time": 0},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 422


async def test_put_filters_working_time_above_maximum_returns_422(app_client):
    """PUT /filters with working_time > 24 → 422."""
    reg = await _register_and_get_tokens(app_client, email="hightime@example.com")

    response = await app_client.put(
        FILTERS_URL,
        json={**_VALID_UPDATE, "working_time": 25},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 422


# --- Validation: PUT /filters with invalid start_time format → 422 ---


async def test_put_filters_invalid_start_time_format_returns_422(app_client):
    """PUT /filters with start_time missing leading zero → 422."""
    reg = await _register_and_get_tokens(app_client, email="badtime1@example.com")

    response = await app_client.put(
        FILTERS_URL,
        json={**_VALID_UPDATE, "start_time": "6:30"},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 422


async def test_put_filters_invalid_start_time_hour_returns_422(app_client):
    """PUT /filters with start_time='25:00' → 422."""
    reg = await _register_and_get_tokens(app_client, email="badtime2@example.com")

    response = await app_client.put(
        FILTERS_URL,
        json={**_VALID_UPDATE, "start_time": "25:00"},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 422


# --- Validation: PUT /filters without JWT → 401 ---


async def test_put_filters_without_jwt_returns_401(app_client):
    """PUT /filters without Authorization header → 401."""
    response = await app_client.put(FILTERS_URL, json=_VALID_UPDATE)

    assert response.status_code == 401
