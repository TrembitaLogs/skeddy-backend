from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_EVENTS_CUTOFF_WEEKS = 8


async def get_unified_events(
    db: AsyncSession,
    user_id: UUID,
    limit: int,
    cursor: tuple[datetime, str, UUID] | None = None,
    since: datetime | None = None,
) -> tuple[list, bool]:
    """Get unified event feed combining rides and credit transactions.

    Executes a UNION ALL query with per-branch LIMIT optimization
    (PRD section 11).  Rides include billing fields; credit events
    exclude RIDE_CHARGE and RIDE_REFUND (those are embedded in ride
    cards via credits_charged / credits_refunded).

    Each returned row has the following named columns:
        event_kind, id, created_at, event_type, ride_data,
        credits_charged, credits_refunded, verification_status,
        credit_type, amount, balance_after, description.
    Unused columns are NULL depending on event_kind.

    Args:
        db: Async database session.
        user_id: The user's UUID.
        limit: Maximum number of events to return.
        cursor: Decoded cursor tuple (created_at, event_kind, event_id),
            or None for the first page.
        since: If provided, only return events created after this time.
            Combined with the 8-week cutoff via max().

    Returns:
        Tuple of (list of Row objects, has_more flag).
    """
    cutoff_8_weeks = datetime.now(UTC) - timedelta(weeks=_EVENTS_CUTOFF_WEEKS)

    if since is not None:
        effective_cutoff = max(since, cutoff_8_weeks)
    else:
        effective_cutoff = cutoff_8_weeks

    fetch_limit = limit + 1  # +1 to detect has_more
    params: dict = {
        "user_id": user_id,
        "effective_cutoff": effective_cutoff,
        "branch_limit": fetch_limit,
        "fetch_limit": fetch_limit,
        "exclude_type_1": "RIDE_CHARGE",
        "exclude_type_2": "RIDE_REFUND",
    }

    cursor_clause_ride = ""
    cursor_clause_credit = ""
    if cursor is not None:
        cursor_ts, cursor_kind, cursor_id = cursor
        params["cursor_ts"] = cursor_ts
        params["cursor_kind"] = cursor_kind
        params["cursor_id"] = cursor_id
        cursor_clause_ride = (
            "AND (created_at, 'ride', id) < (:cursor_ts, :cursor_kind, :cursor_id)"
        )
        cursor_clause_credit = (
            "AND (created_at, 'credit', id) < (:cursor_ts, :cursor_kind, :cursor_id)"
        )

    # Raw SQL is intentional: SQLAlchemy ORM cannot express per-branch ORDER BY +
    # LIMIT inside a CTE UNION ALL, dynamic cursor clauses, and cross-table NULL
    # casts cleanly. Keeping it as raw text is more readable and performs better
    # than a multi-step ORM workaround.
    sql = text(f"""
        WITH ride_events AS (
            SELECT
                'ride'::text AS event_kind,
                id,
                created_at,
                event_type,
                ride_data,
                credits_charged,
                credits_refunded,
                verification_status,
                NULL::varchar(30) AS credit_type,
                NULL::integer AS amount,
                NULL::integer AS balance_after,
                NULL::varchar(500) AS description
            FROM rides
            WHERE user_id = :user_id
              AND created_at > :effective_cutoff
              {cursor_clause_ride}
            ORDER BY created_at DESC, id DESC
            LIMIT :branch_limit
        ),
        credit_events AS (
            SELECT
                'credit'::text AS event_kind,
                id,
                created_at,
                NULL::varchar(20) AS event_type,
                NULL::jsonb AS ride_data,
                NULL::integer AS credits_charged,
                NULL::integer AS credits_refunded,
                NULL::varchar(20) AS verification_status,
                type AS credit_type,
                amount,
                balance_after,
                description
            FROM credit_transactions
            WHERE user_id = :user_id
              AND type NOT IN (:exclude_type_1, :exclude_type_2)
              AND created_at > :effective_cutoff
              {cursor_clause_credit}
            ORDER BY created_at DESC, id DESC
            LIMIT :branch_limit
        )
        SELECT * FROM (
            SELECT * FROM ride_events
            UNION ALL
            SELECT * FROM credit_events
        ) combined
        ORDER BY created_at DESC, event_kind DESC, id DESC
        LIMIT :fetch_limit
    """)

    result = await db.execute(sql, params)
    rows = list(result.fetchall())

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    return rows, has_more
