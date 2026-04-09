# Skeddy Backend

Coordination backend for Skeddy driver apps. Built with **FastAPI**, **PostgreSQL**, **Redis**, and **Firebase**.

## Architecture

The system coordinates two mobile apps:

- **Main App** (user-facing) -- registration, search control, credit management, ride events
- **Search App** (device-side) -- pings, ride offer processing, device pairing

### Tech Stack

| Component    | Technology                          |
|-------------|--------------------------------------|
| Framework   | FastAPI (Python 3.13+)               |
| Database    | PostgreSQL 17 (async via asyncpg)    |
| Cache       | Redis 8                              |
| ORM         | SQLAlchemy 2.0 (async)               |
| Migrations  | Alembic                              |
| Push        | Firebase Cloud Messaging (FCM)       |
| Billing     | Google Play Billing API              |
| Email       | aiosmtplib (SMTP)                    |
| Admin       | SQLAdmin                             |
| Monitoring  | Sentry                               |

### Project Structure

```
app/
  main.py              # Application entry point
  config.py            # Settings (pydantic-settings)
  database.py          # Async SQLAlchemy engine & sessions
  redis.py             # Redis client
  models/              # SQLAlchemy ORM models
  routers/             # FastAPI route handlers
  schemas/             # Pydantic request/response models
  services/            # Business logic layer
  dependencies/        # FastAPI dependency injection
  middleware/          # CORS, CSRF, rate limiter, security headers, logging
  tasks/               # Background tasks (health check, cleanup, reminders)
  admin/               # SQLAdmin panel views
  utils/               # Pagination, code generation
migrations/            # Alembic database migrations
tests/                 # Test suite (pytest, 70%+ coverage)
scripts/               # Deployment, backup, data import scripts
nginx/                 # Nginx reverse proxy configuration
```

## API Endpoints

All endpoints below use the `/api/v1` prefix unless noted otherwise. Full specification: [`../common/.taskmaster/docs/skeddy_api_contract.md`](../common/.taskmaster/docs/skeddy_api_contract.md)

### Authentication

| Method | Endpoint                     | Auth   | Description                     |
|--------|------------------------------|--------|---------------------------------|
| POST   | `/auth/register`             | --     | Register new user               |
| POST   | `/auth/login`                | --     | Login (returns JWT tokens)      |
| POST   | `/auth/logout`               | JWT    | Logout, invalidate tokens       |
| POST   | `/auth/refresh`              | --     | Refresh access token            |
| POST   | `/auth/change-password`      | JWT    | Change password                 |
| POST   | `/auth/request-reset`        | --     | Request password reset (email)  |
| POST   | `/auth/reset-password`       | --     | Confirm password reset          |
| POST   | `/auth/verify-email`         | JWT    | Verify email address            |
| POST   | `/auth/resend-verification`  | JWT    | Resend verification email       |
| POST   | `/auth/change-email`         | JWT    | Initiate email change           |
| DELETE | `/auth/account`              | JWT    | Delete user account             |
| GET    | `/auth/me`                   | JWT    | Get current user profile        |
| POST   | `/auth/search-login`         | --     | Search device login             |

### Profile

| Method | Endpoint    | Auth | Description          |
|--------|-------------|------|----------------------|
| PATCH  | `/profile`  | JWT  | Update user profile  |

### Credits

| Method | Endpoint            | Auth | Description                  |
|--------|---------------------|------|------------------------------|
| POST   | `/credits/purchase` | JWT  | Purchase credits (Google Play)|
| POST   | `/credits/restore`  | JWT  | Restore legacy credits       |

### Search Control

| Method | Endpoint                  | Auth   | Description                 |
|--------|---------------------------|--------|-----------------------------|
| POST   | `/search/start`           | JWT    | Start search (Main App)     |
| POST   | `/search/stop`            | JWT    | Stop search (Main App)      |
| GET    | `/search/status`          | JWT    | Get search status           |
| POST   | `/search/device-override` | Device | Override status (Search App)|

### Search Filters

| Method | Endpoint    | Auth | Description           |
|--------|-------------|------|-----------------------|
| GET    | `/filters`  | JWT  | Get search filters    |
| PUT    | `/filters`  | JWT  | Update search filters |

### FCM

| Method | Endpoint        | Auth | Description          |
|--------|-----------------|------|----------------------|
| POST   | `/fcm/register` | JWT  | Register FCM token   |

### Ping (Search App)

| Method | Endpoint | Auth   | Description                         |
|--------|----------|--------|-------------------------------------|
| POST   | `/ping`  | Device | Heartbeat with status and ride data |

### Rides

| Method | Endpoint        | Auth   | Description              |
|--------|-----------------|--------|--------------------------|
| POST   | `/rides`        | Device | Accept/reject ride offer |
| GET    | `/rides/events` | JWT    | Get ride events          |

### Health (root-level, no `/api/v1` prefix)

| Method | Endpoint  | Auth | Description  |
|--------|-----------|------|--------------|
| GET    | `/health` | --   | Health check |

**Note:** OpenAPI/Swagger UI is disabled in the application (`docs_url=None`). Refer to the API contract linked above or inspect the router source code for endpoint details.

## Development Setup

### Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Docker and Docker Compose
- Python 3.13+

### Quick Start

1. **Clone and install dependencies:**

   ```bash
   cd backend
   uv sync
   ```

2. **Set up environment variables:**

   ```bash
   cp .env.example .env
   # Edit .env with your values (at minimum: DATABASE_URL, REDIS_URL, JWT_SECRET)
   ```

3. **Start infrastructure (database + Redis):**

   ```bash
   docker compose up -d db redis
   ```

4. **Run database migrations:**

   ```bash
   uv run alembic upgrade head
   ```

5. **Start the development server:**

   ```bash
   uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

   The API is available at `http://localhost:8000`.

### Using Docker Compose (Full Stack)

To run the entire stack locally:

```bash
docker compose up -d
```

This starts PostgreSQL, Redis, and the app. The dev override (`docker-compose.override.yml`) exposes ports 5432, 6379, and 8000 to the host.

### Code Quality

Run before every commit:

```bash
uv run ruff check .       # Linting
uv run ruff format --check .  # Formatting
```

Auto-fix:

```bash
uv run ruff check --fix .
uv run ruff format .
```

## Testing

Tests run in **GitHub CI** to ensure a clean environment without local credentials.

```bash
# Run locally (for quick feedback only):
uv run pytest

# With coverage:
uv run pytest --cov=app --cov-report=term-missing --cov-fail-under=70
```

The CI pipeline runs the full test suite on every push. Coverage threshold is **70%**.

All external services (Google Play, Firebase, SMTP) must be mocked in tests. Tests that rely on real credentials will fail in CI.

## Deployment

### Production with Docker Compose

1. **Prepare environment:**

   ```bash
   cp .env.example .env
   # Configure all required variables for production
   ```

2. **Build and start:**

   ```bash
   docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
   ```

   Production mode uses a pre-built image (`ghcr.io/skeddy/backend:latest`) and binds to `127.0.0.1:8000` (behind Nginx).

3. **Run migrations:**

   ```bash
   docker compose exec app alembic upgrade head
   ```

### SSL/TLS Setup

Initialize Let's Encrypt certificates:

```bash
./scripts/init-letsencrypt.sh
```

Nginx configuration is in `nginx/nginx.conf` with TLS 1.2/1.3, HSTS, and security headers.

### Database Backups

Automated backups run every 30 minutes via the `backup` service. Configure optional S3/Spaces upload with:

- `BACKUP_S3_BUCKET`
- `BACKUP_S3_ENDPOINT`
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`

Retention: 7 days (local).

## Environment Variables

### Required

| Variable        | Description                              | Example                                                  |
|-----------------|------------------------------------------|----------------------------------------------------------|
| `DATABASE_URL`  | PostgreSQL async connection string       | `postgresql+asyncpg://skeddy:pass@localhost:5432/skeddy` |
| `REDIS_URL`     | Redis connection string                  | `redis://localhost:6379/0`                                |
| `JWT_SECRET`    | Secret key for JWT signing               | (generate a strong random string)                        |
| `DB_PASSWORD`   | Database password                        | (strong password)                                        |

### Authentication

| Variable                         | Default | Description                     |
|----------------------------------|---------|---------------------------------|
| `JWT_ALGORITHM`                  | HS256   | JWT signing algorithm           |
| `JWT_ACCESS_TOKEN_EXPIRE_HOURS`  | 24      | Access token lifetime (hours)   |
| `JWT_REFRESH_TOKEN_EXPIRE_DAYS`  | 30      | Refresh token lifetime (days)   |

### Email (SMTP)

| Variable         | Description                | Example                       |
|------------------|----------------------------|-------------------------------|
| `EMAIL_HOST`     | SMTP server hostname       | `smtp.gmail.com`              |
| `EMAIL_PORT`     | SMTP port                  | `587`                         |
| `EMAIL_USER`     | SMTP username              | `noreply@skeddy.app`         |
| `EMAIL_PASSWORD` | SMTP password              | (app password)                |
| `EMAIL_FROM`     | From address               | `Skeddy <noreply@skeddy.app>`|

### Firebase & Google Play

| Variable                        | Description                         |
|---------------------------------|-------------------------------------|
| `FIREBASE_CREDENTIALS_PATH`     | Path to Firebase service account    |
| `FIREBASE_CREDENTIALS_JSON`     | Inline Firebase credentials (JSON)  |
| `GOOGLE_PLAY_CREDENTIALS_PATH`  | Path to Google Play service account |
| `GOOGLE_PLAY_CREDENTIALS_JSON`  | Inline Google Play credentials      |
| `GOOGLE_PLAY_PACKAGE_NAME`      | App package ID                      |

### Admin Panel

| Variable            | Default | Description                            |
|---------------------|---------|----------------------------------------|
| `ADMIN_USERNAME`    | admin   | Admin login username                   |
| `ADMIN_PASSWORD`    | --      | Bcrypt hash of admin password          |
| `ADMIN_SECRET_KEY`  | --      | Session secret (32+ chars in prod)     |
| `ADMIN_ALLOWED_IPS` | --      | Optional IP whitelist                  |

### Monitoring & Server

| Variable      | Default | Description                          |
|---------------|---------|--------------------------------------|
| `SENTRY_DSN`  | --      | Sentry error tracking DSN            |
| `CORS_ORIGINS`| --      | Comma-separated allowed origins      |
| `DEBUG`       | false   | Enable debug mode                    |
| `ENVIRONMENT` | --      | Environment name (dev/staging/prod)  |

See `.env.example` for the full list of configuration options.

## License

Proprietary. All rights reserved.
