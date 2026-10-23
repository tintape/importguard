import os

TEST_DATABASE_URL = os.environ.get(
    "IMPORTGUARD_TEST_DATABASE_URL",
    "postgresql+psycopg://importguard:importguard@localhost:5432/importguard_test",
)
