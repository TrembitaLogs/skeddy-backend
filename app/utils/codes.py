import secrets


def generate_six_digit_code() -> str:
    """Generate a random 8-digit numeric code (10000000-99999999).

    Named for backward compatibility; now produces 8 digits for stronger
    brute-force resistance (100M combinations vs 1M).
    """
    return str(secrets.randbelow(90_000_000) + 10_000_000)
