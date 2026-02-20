import random


def generate_six_digit_code() -> str:
    """Generate a random 6-digit numeric code (100000-999999)."""
    return str(random.randint(100000, 999999))
