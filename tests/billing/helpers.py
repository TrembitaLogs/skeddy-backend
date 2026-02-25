"""Utility helpers for billing E2E tests.

Provides assertion helpers and test data generators used across
multiple billing test modules.
"""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction


async def assert_balance(
    db_session: AsyncSession, user_id: uuid.UUID, expected: int
) -> CreditBalance:
    """Assert that a user's credit balance equals the expected value.

    Queries the DB directly (not Redis cache) for authoritative balance.

    Returns the CreditBalance row for further inspection if needed.
    Raises AssertionError if balance does not match or row is missing.
    """
    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == user_id)
    )
    balance_row = result.scalar_one_or_none()
    assert balance_row is not None, f"CreditBalance not found for user {user_id}"
    assert balance_row.balance == expected, (
        f"Expected balance {expected}, got {balance_row.balance} for user {user_id}"
    )
    return balance_row


async def assert_transaction_exists(
    db_session: AsyncSession,
    user_id: uuid.UUID,
    tx_type: str,
    amount: int,
) -> CreditTransaction:
    """Assert that a CreditTransaction with the given type and amount exists.

    Matches on user_id + type + amount. Returns the matching transaction.
    Raises AssertionError if no matching transaction is found.
    """
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user_id,
            CreditTransaction.type == tx_type,
            CreditTransaction.amount == amount,
        )
    )
    tx = result.scalar_one_or_none()
    assert tx is not None, (
        f"CreditTransaction(type={tx_type}, amount={amount}) not found for user {user_id}"
    )
    return tx


def make_ride_hash() -> str:
    """Generate a unique 64-character hex string for ride_hash.

    Uses two UUID4 hex strings concatenated to produce exactly 64 chars,
    matching the SHA-256 format expected by the API.
    """
    return uuid.uuid4().hex + uuid.uuid4().hex
