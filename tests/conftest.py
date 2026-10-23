import os
from pathlib import Path

from dotenv import dotenv_values

# Environment first, then a local .env file, then the docker-compose/CI default.
TEST_DATABASE_URL = (
    os.environ.get("IMPORTGUARD_TEST_DATABASE_URL")
    or dotenv_values(Path(__file__).resolve().parents[1] / ".env").get("IMPORTGUARD_TEST_DATABASE_URL")
    or "postgresql+psycopg://importguard:importguard@localhost:5432/importguard_test"
)
