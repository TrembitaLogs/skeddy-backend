import asyncio
import json
import logging
from dataclasses import dataclass

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.config import settings

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/androidpublisher"]


class GooglePlayVerificationError(Exception):
    """Raised when Google Play purchase verification fails."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


@dataclass(frozen=True)
class GooglePurchaseResult:
    """Structured result from Google Play purchases.products.get API."""

    order_id: str
    purchase_state: int
    consumption_state: int
    acknowledgement_state: int
    purchase_time_millis: str
    already_consumed: bool


class GooglePlayService:
    """Google Play Developer API client for purchase verification.

    Initializes the androidpublisher v3 API service using Service Account
    credentials. Supports two credential sources (checked in order):
    1. GOOGLE_PLAY_CREDENTIALS_PATH — path to a service account JSON key file
    2. GOOGLE_PLAY_CREDENTIALS_JSON — inline JSON string with service account key

    Raises ValueError if neither credential source is configured or if
    GOOGLE_PLAY_PACKAGE_NAME is not set.
    """

    def __init__(self) -> None:
        if settings.GOOGLE_PLAY_CREDENTIALS_PATH:
            credentials = service_account.Credentials.from_service_account_file(
                settings.GOOGLE_PLAY_CREDENTIALS_PATH, scopes=SCOPES
            )
            logger.info("Google Play API initialized with credentials file")
        elif settings.GOOGLE_PLAY_CREDENTIALS_JSON:
            info = json.loads(settings.GOOGLE_PLAY_CREDENTIALS_JSON)
            credentials = service_account.Credentials.from_service_account_info(
                info, scopes=SCOPES
            )
            logger.info("Google Play API initialized with JSON credentials")
        else:
            raise ValueError(
                "Google Play credentials not configured. "
                "Set GOOGLE_PLAY_CREDENTIALS_PATH or GOOGLE_PLAY_CREDENTIALS_JSON."
            )

        if not settings.GOOGLE_PLAY_PACKAGE_NAME:
            raise ValueError("GOOGLE_PLAY_PACKAGE_NAME is required for Google Play API.")

        self._service = build("androidpublisher", "v3", credentials=credentials)
        self._package_name = settings.GOOGLE_PLAY_PACKAGE_NAME

    async def verify_purchase(self, product_id: str, purchase_token: str) -> GooglePurchaseResult:
        """Verify a purchase via Google Play Developer API.

        Calls purchases.products.get() in a thread pool executor because
        google-api-python-client is synchronous.

        Args:
            product_id: The in-app product SKU.
            purchase_token: The purchase token from the client.

        Returns:
            GooglePurchaseResult with order_id, states, and already_consumed flag.

        Raises:
            GooglePlayVerificationError: If the token is invalid (404) or the
                purchase is not in a purchased state.
        """
        loop = asyncio.get_running_loop()

        request = (
            self._service.purchases()
            .products()
            .get(
                packageName=self._package_name,
                productId=product_id,
                token=purchase_token,
            )
        )

        try:
            response = await loop.run_in_executor(None, request.execute)
        except HttpError as e:
            if e.resp.status == 404:
                raise GooglePlayVerificationError(
                    code="INVALID_PURCHASE_TOKEN",
                    message=f"Purchase token not found for product {product_id}",
                ) from e
            raise

        purchase_state = response.get("purchaseState")
        if purchase_state != 0:
            raise GooglePlayVerificationError(
                code="PURCHASE_NOT_COMPLETED",
                message=(f"Purchase not in purchased state: purchaseState={purchase_state}"),
            )

        consumption_state = response.get("consumptionState", 0)

        return GooglePurchaseResult(
            order_id=response.get("orderId", ""),
            purchase_state=purchase_state,
            consumption_state=consumption_state,
            acknowledgement_state=response.get("acknowledgementState", 0),
            purchase_time_millis=response.get("purchaseTimeMillis", ""),
            already_consumed=consumption_state == 1,
        )

    async def consume_purchase(self, product_id: str, purchase_token: str) -> bool:
        """Consume a verified purchase via Google Play Developer API.

        Marks the purchase as consumed so the same SKU can be purchased again
        (consumable product). Must be called after successful verification.

        Uses run_in_executor because google-api-python-client is synchronous.

        Args:
            product_id: The in-app product SKU.
            purchase_token: The purchase token from the client.

        Returns:
            True if consume succeeded, False on any HTTP error.
        """
        loop = asyncio.get_running_loop()

        request = (
            self._service.purchases()
            .products()
            .consume(
                packageName=self._package_name,
                productId=product_id,
                token=purchase_token,
            )
        )

        try:
            await loop.run_in_executor(None, request.execute)
        except HttpError as e:
            status_code = e.resp.status
            logger.error(
                "Google Play consume failed: product_id=%s, status=%s, error=%s",
                product_id,
                status_code,
                str(e),
            )
            if status_code >= 500:
                # Server-side error — let the caller retry
                raise
            # Client error (4xx) — retrying won't help
            return False

        logger.info(
            "Google Play purchase consumed: product_id=%s",
            product_id,
        )
        return True
