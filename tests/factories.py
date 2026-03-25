"""Factory-boy factories for Skeddy models.

These factories use ``factory.Factory`` (not ``SQLAlchemyModelFactory``)
because the project relies on async SQLAlchemy sessions.  Use ``.build()``
to create model instances in memory, then persist them manually::

    user = UserFactory.build()
    db_session.add(user)
    await db_session.flush()

The constant ``TEST_PASSWORD`` contains the plain-text password that
corresponds to the pre-computed ``password_hash`` used by ``UserFactory``.
"""

import hashlib
import uuid
from datetime import UTC, datetime, timedelta

import bcrypt
import factory

from app.models.accept_failure import AcceptFailure
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction
from app.models.paired_device import PairedDevice
from app.models.purchase_order import PurchaseOrder
from app.models.refresh_token import RefreshToken
from app.models.ride import Ride
from app.models.search_filters import SearchFilters
from app.models.search_status import SearchStatus
from app.models.user import User

TEST_PASSWORD = "securePass1"
_TEST_PASSWORD_HASH = bcrypt.hashpw(TEST_PASSWORD.encode(), bcrypt.gensalt(rounds=4)).decode()


class UserFactory(factory.Factory):
    class Meta:
        model = User

    id = factory.LazyFunction(uuid.uuid4)
    email = factory.Sequence(lambda n: f"user{n}@example.com")
    password_hash = _TEST_PASSWORD_HASH
    phone_number = None
    fcm_token = None
    language = "en"


class CreditBalanceFactory(factory.Factory):
    class Meta:
        model = CreditBalance

    id = factory.LazyFunction(uuid.uuid4)
    user_id = factory.LazyFunction(uuid.uuid4)
    balance = 0


class SearchFiltersFactory(factory.Factory):
    class Meta:
        model = SearchFilters

    id = factory.LazyFunction(uuid.uuid4)
    user_id = factory.LazyFunction(uuid.uuid4)
    min_price = 20.0
    start_time = "06:30"
    working_time = 24
    working_days = factory.LazyFunction(lambda: ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"])


class SearchStatusFactory(factory.Factory):
    class Meta:
        model = SearchStatus

    id = factory.LazyFunction(uuid.uuid4)
    user_id = factory.LazyFunction(uuid.uuid4)
    is_active = False


class PairedDeviceFactory(factory.Factory):
    class Meta:
        model = PairedDevice

    id = factory.LazyFunction(uuid.uuid4)
    user_id = factory.LazyFunction(uuid.uuid4)
    device_id = factory.Sequence(lambda n: f"test-device-{n:03d}")
    device_token_hash = factory.LazyFunction(
        lambda: hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    )
    timezone = "America/New_York"
    offline_notified = False
    last_ping_at = None
    last_interval_sent = None
    accessibility_enabled = None
    lyft_running = None
    screen_on = None


class RideFactory(factory.Factory):
    class Meta:
        model = Ride

    id = factory.LazyFunction(uuid.uuid4)
    user_id = factory.LazyFunction(uuid.uuid4)
    idempotency_key = factory.LazyFunction(lambda: str(uuid.uuid4()))
    event_type = "ACCEPTED"
    ride_data = factory.LazyFunction(
        lambda: {
            "price": 25.0,
            "pickup_time": "Tomorrow 6:05AM",
            "pickup_location": "123 Main St",
            "dropoff_location": "456 Oak Ave",
        }
    )
    ride_hash = "a" * 64


class AcceptFailureFactory(factory.Factory):
    class Meta:
        model = AcceptFailure

    id = factory.LazyFunction(uuid.uuid4)
    user_id = factory.LazyFunction(uuid.uuid4)
    reason = "AcceptButtonNotFound"
    ride_price = 25.50
    pickup_time = "Tomorrow 6:05AM"


class CreditTransactionFactory(factory.Factory):
    class Meta:
        model = CreditTransaction

    id = factory.LazyFunction(uuid.uuid4)
    user_id = factory.LazyFunction(uuid.uuid4)
    type = "PURCHASE"
    amount = 10
    balance_after = 10
    reference_id = None
    description = None


class PurchaseOrderFactory(factory.Factory):
    class Meta:
        model = PurchaseOrder

    id = factory.LazyFunction(uuid.uuid4)
    user_id = factory.LazyFunction(uuid.uuid4)
    google_order_id = None
    product_id = "credits_10"
    purchase_token = factory.LazyFunction(lambda: f"token_{uuid.uuid4().hex}")
    credits_amount = 10
    status = "PENDING"
    verified_at = None


class RefreshTokenFactory(factory.Factory):
    class Meta:
        model = RefreshToken

    id = factory.LazyFunction(uuid.uuid4)
    user_id = factory.LazyFunction(uuid.uuid4)
    token_hash = factory.LazyFunction(lambda: hashlib.sha256(uuid.uuid4().bytes).hexdigest())
    expires_at = factory.LazyFunction(lambda: datetime.now(UTC) + timedelta(days=30))
