from __future__ import annotations
from sqlalchemy import engine_from_config, pool
from alembic import context
import os

config = context.config

# Inject DSN from env
user = os.getenv("POSTGRES_USER", "appuser")
password = os.getenv("POSTGRES_PASSWORD", "apppass")
host = os.getenv("POSTGRES_HOST", "db")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "appdb")
config.set_main_option("sqlalchemy.url", f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

# Import metadata; our Base is minimal so autogen isn't used, but this keeps Alembic happy
try:
    import app.models as models
    target_metadata = getattr(models, "Base").metadata
except Exception:
    target_metadata = None

def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    connectable = engine_from_config(config.get_section(config.config_ini_section), prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
