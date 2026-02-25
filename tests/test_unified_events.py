"""Tests for get_unified_events — UNION query for the unified event feed."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from app.models.credit_transaction import CreditTransaction
from app.models.ride import Ride
from app.models.user import User
from app.services.ride_service import _EVENTS_CUTOFF_WEEKS, get_unified_events

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _create_user(db, email="test@example.com") -> User:
    user = User(email=email, password_hash="hashed")
    db.add(user)
    await db.flush()
    return user


def _make_ride(
    user_id,
    *,
    created_at=None,
    credits_charged=0,
    credits_refunded=0,
    verification_status="PENDING",
    **kwargs,
) -> Ride:
    return Ride(
        user_id=user_id,
        idempotency_key=str(uuid4()),
        event_type="ACCEPTED",
        ride_data={
            "price": 25.0,
            "pickup_time": "Tomorrow 6:05AM",
            "pickup_location": "A",
            "dropoff_location": "B",
        },
        ride_hash="a" * 64,
        credits_charged=credits_charged,
        credits_refunded=credits_refunded,
        verification_status=verification_status,
        **({"created_at": created_at} if created_at else {}),
        **kwargs,
    )


def _make_credit_tx(
    user_id,
    *,
    tx_type="PURCHASE",
    amount=10,
    balance_after=10,
    created_at=None,
    description=None,
    reference_id=None,
) -> CreditTransaction:
    return CreditTransaction(
        user_id=user_id,
        type=tx_type,
        amount=amount,
        balance_after=balance_after,
        description=description,
        reference_id=reference_id,
        **({"created_at": created_at} if created_at else {}),
    )


# ===========================================================================
# 1. Query returns ride events with correct fields
# ===========================================================================


@pytest.mark.asyncio
async def test_ride_events_have_correct_fields(db_session):
    """Ride rows must carry event_type, ride_data, credits_charged,
    credits_refunded, and verification_status."""
    user = await _create_user(db_session)
    ride = _make_ride(
        user.id,
        credits_charged=2,
        credits_refunded=1,
        verification_status="CONFIRMED",
    )
    db_session.add(ride)
    await db_session.flush()

    rows, _ = await get_unified_events(db_session, user.id, limit=10)

    assert len(rows) == 1
    row = rows[0]
    assert row.event_kind == "ride"
    assert row.id == ride.id
    assert row.event_type == "ACCEPTED"
    assert row.ride_data is not None
    assert row.credits_charged == 2
    assert row.credits_refunded == 1
    assert row.verification_status == "CONFIRMED"
    # Credit-specific columns should be NULL for rides
    assert row.credit_type is None
    assert row.amount is None
    assert row.balance_after is None


# ===========================================================================
# 2. Query returns credit events (PURCHASE, REGISTRATION_BONUS, ADMIN_ADJUSTMENT)
# ===========================================================================


@pytest.mark.asyncio
async def test_credit_events_types_returned(db_session):
    """PURCHASE, REGISTRATION_BONUS, and ADMIN_ADJUSTMENT must appear."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    txns = [
        _make_credit_tx(
            user.id,
            tx_type="PURCHASE",
            amount=50,
            balance_after=60,
            created_at=now - timedelta(hours=3),
        ),
        _make_credit_tx(
            user.id,
            tx_type="REGISTRATION_BONUS",
            amount=10,
            balance_after=10,
            created_at=now - timedelta(hours=2),
        ),
        _make_credit_tx(
            user.id,
            tx_type="ADMIN_ADJUSTMENT",
            amount=-5,
            balance_after=55,
            description="test adjustment",
            created_at=now - timedelta(hours=1),
        ),
    ]
    db_session.add_all(txns)
    await db_session.flush()

    rows, _ = await get_unified_events(db_session, user.id, limit=10)

    assert len(rows) == 3
    types_returned = {r.credit_type for r in rows}
    assert types_returned == {"PURCHASE", "REGISTRATION_BONUS", "ADMIN_ADJUSTMENT"}

    # Verify credit-specific fields
    for row in rows:
        assert row.event_kind == "credit"
        assert row.amount is not None
        assert row.balance_after is not None
        # Ride-specific columns should be NULL for credit events
        assert row.event_type is None
        assert row.ride_data is None

    # ADMIN_ADJUSTMENT should carry description
    adj_row = next(r for r in rows if r.credit_type == "ADMIN_ADJUSTMENT")
    assert adj_row.description == "test adjustment"


# ===========================================================================
# 3. RIDE_CHARGE and RIDE_REFUND NOT included
# ===========================================================================


@pytest.mark.asyncio
async def test_ride_charge_and_ride_refund_excluded(db_session):
    """RIDE_CHARGE and RIDE_REFUND transactions must NOT appear as credit cards."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    ride_id = uuid4()
    txns = [
        _make_credit_tx(
            user.id,
            tx_type="RIDE_CHARGE",
            amount=-2,
            balance_after=8,
            reference_id=ride_id,
            created_at=now - timedelta(hours=2),
        ),
        _make_credit_tx(
            user.id,
            tx_type="RIDE_REFUND",
            amount=2,
            balance_after=10,
            reference_id=ride_id,
            created_at=now - timedelta(hours=1),
        ),
        _make_credit_tx(user.id, tx_type="PURCHASE", amount=10, balance_after=20, created_at=now),
    ]
    db_session.add_all(txns)
    await db_session.flush()

    rows, _ = await get_unified_events(db_session, user.id, limit=10)

    assert len(rows) == 1
    assert rows[0].credit_type == "PURCHASE"


# ===========================================================================
# 4. Sorted by created_at DESC
# ===========================================================================


@pytest.mark.asyncio
async def test_sorted_by_created_at_desc(db_session):
    """Events must be sorted newest-first across both rides and credits."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    ride1 = _make_ride(user.id, created_at=now - timedelta(hours=4))
    tx1 = _make_credit_tx(
        user.id,
        tx_type="PURCHASE",
        amount=50,
        balance_after=60,
        created_at=now - timedelta(hours=3),
    )
    ride2 = _make_ride(user.id, created_at=now - timedelta(hours=2))
    tx2 = _make_credit_tx(
        user.id,
        tx_type="REGISTRATION_BONUS",
        amount=10,
        balance_after=10,
        created_at=now - timedelta(hours=1),
    )

    db_session.add_all([ride1, tx1, ride2, tx2])
    await db_session.flush()

    rows, _ = await get_unified_events(db_session, user.id, limit=10)

    assert len(rows) == 4
    timestamps = [r.created_at for r in rows]
    assert timestamps == sorted(timestamps, reverse=True)

    # Verify interleaving: newest credit, then ride, then credit, then ride
    assert rows[0].event_kind == "credit"
    assert rows[0].id == tx2.id
    assert rows[1].event_kind == "ride"
    assert rows[1].id == ride2.id
    assert rows[2].event_kind == "credit"
    assert rows[2].id == tx1.id
    assert rows[3].event_kind == "ride"
    assert rows[3].id == ride1.id


# ===========================================================================
# 5. Cursor filtering works correctly
# ===========================================================================


@pytest.mark.asyncio
async def test_cursor_returns_next_page(db_session):
    """Providing a cursor should skip events at or after the cursor position."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    # Create 4 events, 1 hour apart
    events_data = []
    for i in range(4):
        ride = _make_ride(user.id, created_at=now - timedelta(hours=4 - i))
        events_data.append(ride)
    db_session.add_all(events_data)
    await db_session.flush()

    # First page
    rows_page1, has_more = await get_unified_events(db_session, user.id, limit=2)
    assert len(rows_page1) == 2
    assert has_more is True

    # Build cursor from last item on page 1
    last = rows_page1[-1]
    cursor = (last.created_at, last.event_kind, last.id)

    # Second page via cursor
    rows_page2, has_more2 = await get_unified_events(db_session, user.id, limit=2, cursor=cursor)
    assert len(rows_page2) == 2
    assert has_more2 is False

    # No overlap between pages
    page1_ids = {r.id for r in rows_page1}
    page2_ids = {r.id for r in rows_page2}
    assert page1_ids.isdisjoint(page2_ids)

    # All 4 events covered
    assert len(page1_ids | page2_ids) == 4


@pytest.mark.asyncio
async def test_cursor_with_mixed_event_kinds(db_session):
    """Cursor should work correctly when ride and credit events are interleaved."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    # Interleave rides and credits
    ride = _make_ride(user.id, created_at=now - timedelta(hours=2))
    tx = _make_credit_tx(
        user.id,
        tx_type="PURCHASE",
        amount=10,
        balance_after=10,
        created_at=now - timedelta(hours=1),
    )
    db_session.add_all([ride, tx])
    await db_session.flush()

    # Page 1: get newest event (the credit tx)
    rows_page1, has_more = await get_unified_events(db_session, user.id, limit=1)
    assert len(rows_page1) == 1
    assert rows_page1[0].event_kind == "credit"
    assert has_more is True

    # Page 2: cursor after credit event should return the ride
    last = rows_page1[0]
    cursor = (last.created_at, last.event_kind, last.id)
    rows_page2, has_more2 = await get_unified_events(db_session, user.id, limit=1, cursor=cursor)
    assert len(rows_page2) == 1
    assert rows_page2[0].event_kind == "ride"
    assert rows_page2[0].id == ride.id
    assert has_more2 is False


# ===========================================================================
# 6. 8-week cutoff filters old records
# ===========================================================================


@pytest.mark.asyncio
async def test_cutoff_8_weeks_filters_old_events(db_session):
    """Events older than 8 weeks must not appear."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    old_ride = _make_ride(user.id, created_at=now - timedelta(weeks=_EVENTS_CUTOFF_WEEKS + 1))
    recent_ride = _make_ride(user.id, created_at=now - timedelta(hours=1))
    old_tx = _make_credit_tx(
        user.id,
        tx_type="PURCHASE",
        amount=10,
        balance_after=10,
        created_at=now - timedelta(weeks=_EVENTS_CUTOFF_WEEKS + 1),
    )
    recent_tx = _make_credit_tx(
        user.id,
        tx_type="REGISTRATION_BONUS",
        amount=10,
        balance_after=10,
        created_at=now - timedelta(hours=2),
    )

    db_session.add_all([old_ride, recent_ride, old_tx, recent_tx])
    await db_session.flush()

    rows, _ = await get_unified_events(db_session, user.id, limit=10)

    assert len(rows) == 2
    returned_ids = {r.id for r in rows}
    assert recent_ride.id in returned_ids
    assert recent_tx.id in returned_ids
    assert old_ride.id not in returned_ids
    assert old_tx.id not in returned_ids


# ===========================================================================
# 7. Since filter narrows results
# ===========================================================================


@pytest.mark.asyncio
async def test_since_filter_narrows_results(db_session):
    """Since parameter should exclude events before the specified datetime."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    older = _make_ride(user.id, created_at=now - timedelta(days=5))
    newer = _make_ride(user.id, created_at=now - timedelta(days=1))
    tx_older = _make_credit_tx(
        user.id,
        tx_type="PURCHASE",
        amount=10,
        balance_after=10,
        created_at=now - timedelta(days=4),
    )
    tx_newer = _make_credit_tx(
        user.id,
        tx_type="REGISTRATION_BONUS",
        amount=10,
        balance_after=20,
        created_at=now - timedelta(hours=12),
    )

    db_session.add_all([older, newer, tx_older, tx_newer])
    await db_session.flush()

    since = now - timedelta(days=2)
    rows, _ = await get_unified_events(db_session, user.id, limit=10, since=since)

    assert len(rows) == 2
    returned_ids = {r.id for r in rows}
    assert newer.id in returned_ids
    assert tx_newer.id in returned_ids


@pytest.mark.asyncio
async def test_since_filter_works_for_both_types(db_session):
    """Since should filter both ride and credit events equally."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)
    since = now - timedelta(days=3)

    # Before since
    old_ride = _make_ride(user.id, created_at=now - timedelta(days=5))
    old_tx = _make_credit_tx(
        user.id,
        tx_type="PURCHASE",
        amount=10,
        balance_after=10,
        created_at=now - timedelta(days=4),
    )

    # After since
    new_ride = _make_ride(user.id, created_at=now - timedelta(days=1))
    new_tx = _make_credit_tx(
        user.id,
        tx_type="REGISTRATION_BONUS",
        amount=10,
        balance_after=20,
        created_at=now - timedelta(days=2),
    )

    db_session.add_all([old_ride, old_tx, new_ride, new_tx])
    await db_session.flush()

    rows, _ = await get_unified_events(db_session, user.id, limit=10, since=since)

    assert len(rows) == 2
    kinds = {r.event_kind for r in rows}
    assert kinds == {"ride", "credit"}


# ===========================================================================
# 8. Since older than 8 weeks → cutoff has priority
# ===========================================================================


@pytest.mark.asyncio
async def test_since_older_than_cutoff_uses_cutoff(db_session):
    """When since is older than 8 weeks, the 8-week cutoff takes priority."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    # Event at 7 weeks ago (within cutoff, should appear)
    within_cutoff = _make_ride(user.id, created_at=now - timedelta(weeks=_EVENTS_CUTOFF_WEEKS - 1))
    # Event at 9 weeks ago (outside cutoff, should NOT appear)
    outside_cutoff = _make_ride(
        user.id, created_at=now - timedelta(weeks=_EVENTS_CUTOFF_WEEKS + 1)
    )
    db_session.add_all([within_cutoff, outside_cutoff])
    await db_session.flush()

    # since = 20 weeks ago — much older than the 8-week cutoff
    ancient_since = now - timedelta(weeks=20)
    rows, _ = await get_unified_events(db_session, user.id, limit=10, since=ancient_since)

    assert len(rows) == 1
    assert rows[0].id == within_cutoff.id


# ===========================================================================
# Edge cases
# ===========================================================================


@pytest.mark.asyncio
async def test_empty_result_for_no_events(db_session):
    """Should return empty list and has_more=False with no data."""
    user = await _create_user(db_session)

    rows, has_more = await get_unified_events(db_session, user.id, limit=10)

    assert rows == []
    assert has_more is False


@pytest.mark.asyncio
async def test_has_more_flag_exact_limit(db_session):
    """has_more should be False when events exactly equal the limit."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    for i in range(3):
        db_session.add(_make_ride(user.id, created_at=now - timedelta(hours=i + 1)))
    await db_session.flush()

    rows, has_more = await get_unified_events(db_session, user.id, limit=3)

    assert len(rows) == 3
    assert has_more is False


@pytest.mark.asyncio
async def test_has_more_flag_more_than_limit(db_session):
    """has_more should be True when more events exist beyond the limit."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    for i in range(5):
        db_session.add(_make_ride(user.id, created_at=now - timedelta(hours=i + 1)))
    await db_session.flush()

    rows, has_more = await get_unified_events(db_session, user.id, limit=3)

    assert len(rows) == 3
    assert has_more is True


@pytest.mark.asyncio
async def test_events_scoped_to_user(db_session):
    """Should only return events for the specified user."""
    user_a = await _create_user(db_session, "a@example.com")
    user_b = await _create_user(db_session, "b@example.com")
    now = datetime.now(UTC)

    db_session.add(_make_ride(user_a.id, created_at=now - timedelta(hours=1)))
    db_session.add(_make_ride(user_b.id, created_at=now - timedelta(hours=2)))
    db_session.add(
        _make_credit_tx(
            user_a.id,
            tx_type="PURCHASE",
            amount=10,
            balance_after=10,
            created_at=now - timedelta(hours=3),
        )
    )
    await db_session.flush()

    rows_a, _ = await get_unified_events(db_session, user_a.id, limit=10)
    rows_b, _ = await get_unified_events(db_session, user_b.id, limit=10)

    assert len(rows_a) == 2
    assert len(rows_b) == 1


# ===========================================================================
# Same timestamp: stable sort order (event_kind DESC, id DESC)
# ===========================================================================


@pytest.mark.asyncio
async def test_same_timestamp_rides_before_credits(db_session):
    """At equal created_at, rides sort before credits (event_kind DESC)."""
    user = await _create_user(db_session)
    ts = datetime.now(UTC) - timedelta(hours=1)

    ride = _make_ride(user.id, created_at=ts)
    tx = _make_credit_tx(user.id, tx_type="PURCHASE", amount=10, balance_after=10, created_at=ts)
    db_session.add_all([ride, tx])
    await db_session.flush()

    rows, _ = await get_unified_events(db_session, user.id, limit=10)

    assert len(rows) == 2
    assert rows[0].event_kind == "ride"
    assert rows[0].id == ride.id
    assert rows[1].event_kind == "credit"
    assert rows[1].id == tx.id


@pytest.mark.asyncio
async def test_same_timestamp_multiple_events_stable_order(db_session):
    """Multiple events at the same timestamp maintain stable order across calls."""
    user = await _create_user(db_session)
    ts = datetime.now(UTC) - timedelta(hours=1)

    ride1 = _make_ride(user.id, created_at=ts)
    ride2 = _make_ride(user.id, created_at=ts)
    tx1 = _make_credit_tx(user.id, tx_type="PURCHASE", amount=10, balance_after=10, created_at=ts)
    tx2 = _make_credit_tx(
        user.id, tx_type="REGISTRATION_BONUS", amount=5, balance_after=15, created_at=ts
    )
    db_session.add_all([ride1, ride2, tx1, tx2])
    await db_session.flush()

    rows_first, _ = await get_unified_events(db_session, user.id, limit=10)
    rows_second, _ = await get_unified_events(db_session, user.id, limit=10)

    assert len(rows_first) == 4
    # Rides come first (event_kind DESC: 'ride' > 'credit')
    assert rows_first[0].event_kind == "ride"
    assert rows_first[1].event_kind == "ride"
    assert rows_first[2].event_kind == "credit"
    assert rows_first[3].event_kind == "credit"
    # Order is stable between calls
    ids_first = [r.id for r in rows_first]
    ids_second = [r.id for r in rows_second]
    assert ids_first == ids_second


# ===========================================================================
# Cursor at type boundary (ride → credit at same timestamp)
# ===========================================================================


@pytest.mark.asyncio
async def test_cursor_at_type_boundary_same_timestamp(db_session):
    """Cursor on last ride at timestamp T returns credit events at same T."""
    user = await _create_user(db_session)
    ts = datetime.now(UTC) - timedelta(hours=1)

    ride = _make_ride(user.id, created_at=ts)
    tx = _make_credit_tx(user.id, tx_type="PURCHASE", amount=10, balance_after=10, created_at=ts)
    db_session.add_all([ride, tx])
    await db_session.flush()

    # Page 1: get the ride (sorts first at same timestamp)
    rows_p1, has_more = await get_unified_events(db_session, user.id, limit=1)
    assert len(rows_p1) == 1
    assert rows_p1[0].event_kind == "ride"
    assert has_more is True

    # Page 2: cursor from ride should return the credit at same timestamp
    last = rows_p1[0]
    cursor = (last.created_at, last.event_kind, last.id)
    rows_p2, has_more2 = await get_unified_events(db_session, user.id, limit=1, cursor=cursor)
    assert len(rows_p2) == 1
    assert rows_p2[0].event_kind == "credit"
    assert rows_p2[0].id == tx.id
    assert has_more2 is False


@pytest.mark.asyncio
async def test_cursor_same_timestamp_full_pagination(db_session):
    """Paginate one-by-one through 4 events sharing the same timestamp."""
    user = await _create_user(db_session)
    ts = datetime.now(UTC) - timedelta(hours=1)

    ride1 = _make_ride(user.id, created_at=ts)
    ride2 = _make_ride(user.id, created_at=ts)
    tx1 = _make_credit_tx(user.id, tx_type="PURCHASE", amount=10, balance_after=10, created_at=ts)
    tx2 = _make_credit_tx(
        user.id, tx_type="REGISTRATION_BONUS", amount=5, balance_after=15, created_at=ts
    )
    db_session.add_all([ride1, ride2, tx1, tx2])
    await db_session.flush()

    collected_ids = []
    cursor = None
    pages = 0

    while True:
        rows, has_more = await get_unified_events(db_session, user.id, limit=1, cursor=cursor)
        if not rows:
            break
        collected_ids.append(rows[0].id)
        last = rows[0]
        cursor = (last.created_at, last.event_kind, last.id)
        pages += 1
        if not has_more:
            break

    # All 4 events visited, no duplicates
    assert len(collected_ids) == 4
    assert len(set(collected_ids)) == 4
    assert pages == 4


# ===========================================================================
# Cursor past all data → empty result
# ===========================================================================


@pytest.mark.asyncio
async def test_cursor_past_all_data_returns_empty(db_session):
    """Cursor with a timestamp older than all events returns empty result."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    db_session.add(_make_ride(user.id, created_at=now - timedelta(hours=1)))
    db_session.add(
        _make_credit_tx(
            user.id,
            tx_type="PURCHASE",
            amount=10,
            balance_after=10,
            created_at=now - timedelta(hours=2),
        )
    )
    await db_session.flush()

    # Cursor far in the past (but still within 8-week cutoff)
    old_cursor = (now - timedelta(weeks=4), "credit", uuid4())
    rows, has_more = await get_unified_events(db_session, user.id, limit=10, cursor=old_cursor)

    assert rows == []
    assert has_more is False


# ===========================================================================
# Since + cursor combination
# ===========================================================================


@pytest.mark.asyncio
async def test_since_combined_with_cursor_pagination(db_session):
    """Since filter and cursor work together for paginated filtered results."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    # Old events (before since) — should be excluded
    db_session.add(_make_ride(user.id, created_at=now - timedelta(days=10)))
    db_session.add(
        _make_credit_tx(
            user.id,
            tx_type="PURCHASE",
            amount=5,
            balance_after=5,
            created_at=now - timedelta(days=9),
        )
    )

    # Recent events (after since) — should be included
    recent_ride1 = _make_ride(user.id, created_at=now - timedelta(days=2))
    recent_tx = _make_credit_tx(
        user.id,
        tx_type="REGISTRATION_BONUS",
        amount=10,
        balance_after=15,
        created_at=now - timedelta(days=1),
    )
    recent_ride2 = _make_ride(user.id, created_at=now - timedelta(hours=6))

    db_session.add_all([recent_ride1, recent_tx, recent_ride2])
    await db_session.flush()

    since = now - timedelta(days=3)

    # Page 1 with since
    rows_p1, has_more = await get_unified_events(db_session, user.id, limit=2, since=since)
    assert len(rows_p1) == 2
    assert has_more is True

    # Page 2 with since + cursor
    last = rows_p1[-1]
    cursor = (last.created_at, last.event_kind, last.id)
    rows_p2, has_more2 = await get_unified_events(
        db_session, user.id, limit=2, cursor=cursor, since=since
    )
    assert len(rows_p2) == 1
    assert has_more2 is False

    # Total: 3 recent events, 0 old events
    all_ids = {r.id for r in rows_p1} | {r.id for r in rows_p2}
    assert len(all_ids) == 3
    assert recent_ride1.id in all_ids
    assert recent_tx.id in all_ids
    assert recent_ride2.id in all_ids


# ===========================================================================
# Edge case: only ride events, no credit events
# ===========================================================================


@pytest.mark.asyncio
async def test_only_ride_events_no_credits(db_session):
    """Feed works when user has rides but no eligible credit transactions."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    ride1 = _make_ride(
        user.id,
        created_at=now - timedelta(hours=2),
        credits_charged=2,
        verification_status="CONFIRMED",
    )
    ride2 = _make_ride(
        user.id,
        created_at=now - timedelta(hours=1),
        credits_charged=1,
        verification_status="PENDING",
    )
    # RIDE_CHARGE is excluded from credit events
    ride_charge = _make_credit_tx(
        user.id,
        tx_type="RIDE_CHARGE",
        amount=-2,
        balance_after=8,
        created_at=now - timedelta(hours=2),
    )

    db_session.add_all([ride1, ride2, ride_charge])
    await db_session.flush()

    rows, has_more = await get_unified_events(db_session, user.id, limit=10)

    assert len(rows) == 2
    assert all(r.event_kind == "ride" for r in rows)
    assert has_more is False


# ===========================================================================
# Edge case: only credit events, no rides
# ===========================================================================


@pytest.mark.asyncio
async def test_only_credit_events_no_rides(db_session):
    """Feed works when user has credit transactions but no rides."""
    user = await _create_user(db_session)
    now = datetime.now(UTC)

    tx1 = _make_credit_tx(
        user.id,
        tx_type="REGISTRATION_BONUS",
        amount=10,
        balance_after=10,
        created_at=now - timedelta(hours=3),
    )
    tx2 = _make_credit_tx(
        user.id,
        tx_type="PURCHASE",
        amount=50,
        balance_after=60,
        created_at=now - timedelta(hours=2),
    )
    tx3 = _make_credit_tx(
        user.id,
        tx_type="ADMIN_ADJUSTMENT",
        amount=-5,
        balance_after=55,
        description="correction",
        created_at=now - timedelta(hours=1),
    )

    db_session.add_all([tx1, tx2, tx3])
    await db_session.flush()

    rows, has_more = await get_unified_events(db_session, user.id, limit=10)

    assert len(rows) == 3
    assert all(r.event_kind == "credit" for r in rows)
    types_returned = [r.credit_type for r in rows]
    # Sorted DESC by created_at
    assert types_returned == ["ADMIN_ADJUSTMENT", "PURCHASE", "REGISTRATION_BONUS"]
    assert has_more is False
