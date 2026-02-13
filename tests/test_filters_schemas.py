import pytest
from pydantic import ValidationError

from app.schemas.filters import FiltersResponse, FiltersUpdateRequest


class TestFiltersResponse:
    """Test FiltersResponse default values."""

    def test_defaults(self):
        """FiltersResponse creates with correct default values."""
        response = FiltersResponse()
        assert response.min_price == 20.0
        assert response.start_time == "06:30"
        assert response.working_time == 24
        assert response.working_days == [
            "MON",
            "TUE",
            "WED",
            "THU",
            "FRI",
            "SAT",
            "SUN",
        ]


class TestFiltersUpdateRequest:
    """Test FiltersUpdateRequest validation rules."""

    def test_valid_data(self):
        """Valid filter data is accepted."""
        request = FiltersUpdateRequest(
            min_price=25.0,
            start_time="07:00",
            working_time=12,
            working_days=["MON", "TUE", "WED", "THU", "FRI"],
        )
        assert request.min_price == 25.0
        assert request.start_time == "07:00"
        assert request.working_time == 12
        assert request.working_days == ["MON", "TUE", "WED", "THU", "FRI"]

    def test_min_price_below_minimum(self):
        """min_price < 10 raises ValidationError."""
        with pytest.raises(ValidationError):
            FiltersUpdateRequest(
                min_price=9.99,
                start_time="06:30",
                working_time=24,
                working_days=["MON"],
            )

    def test_min_price_above_maximum(self):
        """min_price > 100000 raises ValidationError."""
        with pytest.raises(ValidationError):
            FiltersUpdateRequest(
                min_price=100001,
                start_time="06:30",
                working_time=24,
                working_days=["MON"],
            )

    def test_working_days_empty(self):
        """Empty working_days raises ValidationError."""
        with pytest.raises(ValidationError):
            FiltersUpdateRequest(
                min_price=20.0,
                start_time="06:30",
                working_time=24,
                working_days=[],
            )

    def test_working_days_invalid(self):
        """Invalid working day raises ValidationError."""
        with pytest.raises(ValidationError):
            FiltersUpdateRequest(
                min_price=20.0,
                start_time="06:30",
                working_time=24,
                working_days=["INVALID"],
            )

    def test_start_time_invalid_hour(self):
        """start_time='25:00' raises ValidationError."""
        with pytest.raises(ValidationError):
            FiltersUpdateRequest(
                min_price=20.0,
                start_time="25:00",
                working_time=24,
                working_days=["MON"],
            )

    def test_start_time_no_leading_zero(self):
        """start_time='6:30' without leading zero raises ValidationError."""
        with pytest.raises(ValidationError):
            FiltersUpdateRequest(
                min_price=20.0,
                start_time="6:30",
                working_time=24,
                working_days=["MON"],
            )

    def test_start_time_valid(self):
        """start_time='06:30' is accepted."""
        request = FiltersUpdateRequest(
            min_price=20.0,
            start_time="06:30",
            working_time=24,
            working_days=["MON"],
        )
        assert request.start_time == "06:30"
