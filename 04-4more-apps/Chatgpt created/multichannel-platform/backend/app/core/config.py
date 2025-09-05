import os
class Settings:
    db_user = os.getenv("POSTGRES_USER", "appuser")
    db_password = os.getenv("POSTGRES_PASSWORD", "apppass")
    db_host = os.getenv("POSTGRES_HOST", "db")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "appdb")
settings = Settings()
