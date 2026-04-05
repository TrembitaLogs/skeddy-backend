# Claude Code Instructions

<!-- ## Task Master AI Instructions
**Import Task Master's development workflow commands and guidelines, treat as if import is in the main CLAUDE.md file.**
@./.taskmaster/CLAUDE.md -->

- PRD документ: .taskmaster/docs/skeddy_server.prd.md
- API Contract: ../common/.taskmaster/docs/skeddy_api_contract.md
- Завжди виконуй тестову стратегію для задачі перед її завершенням.
- dependencies для subtasks відносяться до задач в середені батьківської задачі. наприклад батьківська задача 4 підзадача 3 залежить від 1 та 2 -> треба читати як залежність від задач 4.1 та 4.2
- Усі коментарі в коді мають бути англійською мовою. Ніяких інших мов в коді бути немає.
- Локалізація наразі тільки англійська мова. Ніякого хардкоду в коді для UI бути не має!!!

## Development Environment
- Розробка ведеться локально в Docker (docker-compose).
- Для управління Python-залежностями та віртуальним середовищем використовується **uv** (замість pip/pip-tools).
- Використовуй uv команди замість pip install: uv add, uv sync, uv run тощо.
- Використовуй останні стабільні версії Python, FastAPI та всіх інших бібліотек за можливістю.
- Продакшн сервер ssh skapi

## Working with Code
- Edit code locally in `/projects/skeddy/backend`
- All git operations (commit, push, branch) from this directory
- Do NOT use SSH to other containers for testing

## Running Tests
- Do NOT run tests locally — local environment has credentials (GOOGLE_PLAY_CREDENTIALS_JSON etc.) that mask real problems
- Push code and let GitHub CI run the tests
- CI environment is clean and catches missing mocks
- If CI fails: `gh run view <RUN_ID> --log-failed` — read errors, fix, push again
- If CI passes: task status → in_review

## Quality Checks
Run before every push:
- `uv run ruff check .`
- `uv run ruff format --check .`

## PR Workflow
1. Create a feature branch (`git checkout -b <branch-name>`)
2. Commit and push
3. `gh pr create --title "..." --body "..."`
4. `gh pr checks <PR_NUMBER> --watch`
5. If CI fails: `gh run view <RUN_ID> --log-failed` — read errors, fix, push again
6. If CI passes: task status → in_review
7. Never merge PRs independently — wait for board approval
8. Delete branches after PR merge

## Important Rules
- All tests MUST mock external services (Google Play, FCM, SMTP)
- Tests that pass locally but fail on CI = broken mocks
- Always use a separate branch per task — never commit directly to main
- Run `ruff check` and `ruff format` before every push

## CI Testing Rules
- Tests MUST NOT rely on environment variables for credentials (e.g. GOOGLE_PLAY_CREDENTIALS_JSON, GOOGLE_PLAY_CREDENTIALS_PATH)
- Google Play service MUST always be mocked via `_create_google_play_service` — never let the real constructor run in tests
- Tests MUST pass in a clean CI environment without any secrets or credential files
- Any endpoint that injects GooglePlayService as a FastAPI dependency requires the mock even if the test does not exercise the Google Play code path (the dependency is resolved before the endpoint body runs)
