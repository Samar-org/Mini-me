# Multichannel Commerce Ops Platform (Skeleton)

Production-ready scaffold for a **multi-tenant** inventory, listings, orders, and analytics platform.

## What you get
- **Backend**: FastAPI + SQLAlchemy + Alembic + Celery + Redis + Postgres
- **RLS**-ready Postgres policies using a session variable (`app.tenant_id`)
- **JWT Auth** (access + refresh), role-based (admin/manager/operator)
- **Outbox pattern** + worker to process async marketplace publishes
- **Frontend**: Next.js (App Router) + Tailwind + basic pages & auth flow
- **Docker Compose** for local dev
- **Makefile** for common tasks

> This is a foundation to connect real marketplace APIs (Amazon/eBay/Shopify/Etsy). The code includes stubs and queues where those integrations should live.

---

## Quick Start (Docker)
1. Copy `.env.example` to `.env` and adjust values.
2. Build & run:
   ```bash
   make up
   ```
3. Run DB migrations:
   ```bash
   make migrate
   ```
4. Open services:
   - Backend API: http://localhost:8000 (docs at `/docs`)
   - Frontend: http://localhost:3000

## Useful Commands
```bash
make up           # build & start all services
make down         # stop and remove
make logs         # tail logs
make migrate      # alembic upgrade head
make makemigration msg="add something"
make shell        # backend container shell
```

## Architecture
```
frontend (Next) ── JWT ─▶ backend (FastAPI)
                                  │
                                  ├─ Postgres (RLS per tenant)
                                  ├─ Redis (cache & Celery broker)
                                  └─ Celery worker (outbox jobs, scrapers, sync)
```
---
### Next Steps
- Wire marketplace OAuth and publish flows into `app/services/` & `app/workers/tasks.py`
- Expand schemas & validators for channel-specific rules
- Add OpenTelemetry traces & dashboards
- Harden RLS and index strategy per your data volume
