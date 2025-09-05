from __future__ import annotations
import os
import time
import json
import re
import httpx
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from passlib.context import CryptContext
import jwt

# Optional Airtable
try:
    from pyairtable import Api as AirtableApi
except Exception:
    AirtableApi = None  # type: ignore

from dotenv import load_dotenv
load_dotenv()

# -----------------------------
# Config
# -----------------------------
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

# Demo users: either provide USER_EMAIL/USER_PASSWORD or USERS_JSON like
# [{"email":"ops@4more.com","password_hash":"bcrypt$..."}]
USER_EMAIL = os.getenv("USER_EMAIL")
USER_PASSWORD_HASH = os.getenv("USER_PASSWORD_HASH")  # bcrypt hash preferred
USERS_JSON = os.getenv("USERS_JSON")

# Airtable config (one table per workflow, override via env)
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_BID4MORE = os.getenv("AIRTABLE_TABLE_BID4MORE", "Items-Bid4more")
AIRTABLE_TABLE_BIN4MORE = os.getenv("AIRTABLE_TABLE_BIN4MORE", "Items-Bin4more")
AIRTABLE_TABLE_PAY4MORE = os.getenv("AIRTABLE_TABLE_PAY4MORE", "Items-Pay4more")
AIRTABLE_TABLE_CATALOGUE = os.getenv("AIRTABLE_TABLE_CATALOGUE", "Items-Catalogue")
AIRTABLE_TABLE_FIX4MORE = os.getenv("AIRTABLE_TABLE_FIX4MORE", "Items-Fix4more")

# Optional UPC providers
UPCITEMDB_API_KEY = os.getenv("UPCITEMDB_API_KEY")
BARCODELOOKUP_API_KEY = os.getenv("BARCODELOOKUP_API_KEY")
OPENFOODFACTS_ENABLED = os.getenv("OPENFOODFACTS_ENABLED", "0") == "1"

# Timeouts
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "15"))

# -----------------------------
# App setup
# -----------------------------
app = FastAPI(title="4more Backend", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Auth
# -----------------------------
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

class User(BaseModel):
    email: str

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: User

# In-memory user store
_users: Dict[str, str] = {}
if USERS_JSON:
    try:
        for u in json.loads(USERS_JSON):
            _users[u["email"].lower()] = u["password_hash"]
    except Exception:
        pass
if USER_EMAIL and USER_PASSWORD_HASH:
    _users[USER_EMAIL.lower()] = USER_PASSWORD_HASH

if not _users:
    # Dev fallback: user=demo@4more.com / password=demo (NOT for production)
    _users["demo@4more.com"] = pwd_context.hash("demo")


def verify_password(plain_password: str, password_hash: str) -> bool:
    try:
        if password_hash.startswith("bcrypt$") or password_hash.startswith("$2"):
            return pwd_context.verify(plain_password, password_hash)
        # Allow plain text ONLY for quick local tests
        return plain_password == password_hash
    except Exception:
        return False


def authenticate_user(email: str, password: str) -> Optional[User]:
    ph = _users.get(email.lower())
    if not ph:
        return None
    if not verify_password(password, ph):
        return None
    return User(email=email)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGO)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
    except Exception:
        raise credentials_exception
    if email.lower() not in _users:
        raise credentials_exception
    return User(email=email)


@app.post("/auth/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect email or password")
    token = create_access_token({"sub": user.email})
    return Token(access_token=token, user=user)

# -----------------------------
# Models
# -----------------------------
class LookupItem(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    images: List[str] = []
    dimensions: Optional[str] = None
    weight: Optional[str] = None
    source: Optional[str] = None
    barcode: Optional[str] = None
    url: Optional[str] = None

class AirtableAddRequest(BaseModel):
    mode: str
    item: LookupItem

# -----------------------------
# Helpers: Normalization
# -----------------------------
FLOAT_RE = re.compile(r"([0-9]+[\.,]?[0-9]*)")

def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    m = FLOAT_RE.search(str(value))
    return float(m.group(1).replace(",", ".")) if m else None

# -----------------------------
# Providers: UPC / URL lookup
# -----------------------------
async def provider_upcitemdb(barcode: str) -> Optional[LookupItem]:
    if not UPCITEMDB_API_KEY:
        return None
    headers = {"user_key": UPCITEMDB_API_KEY}
    url = f"https://api.upcitemdb.com/prod/trial/lookup?upc={barcode}"
    # Note: if you have a paid plan, change the host/path above.
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get(url, headers=headers)
        if r.status_code != 200:
            return None
        data = r.json()
        items = data.get("items") or []
        if not items:
            return None
        it = items[0]
        return LookupItem(
            name=it.get("title"),
            description=(it.get("description") or it.get("brand")),
            price=_to_float(it.get("lowest_recorded_price")),
            currency=None,
            images=it.get("images") or [],
            dimensions=it.get("size"),
            weight=None,
            source="upcitemdb",
            barcode=barcode,
            url=(it.get("offer") or (it.get("offers") or [{}])[0].get("link")) if it.get("offers") else None,
        )
    except Exception:
        return None


async def provider_barcodelookup(barcode: str) -> Optional[LookupItem]:
    if not BARCODELOOKUP_API_KEY:
        return None
    url = f"https://api.barcodelookup.com/v3/products?barcode={barcode}&key={BARCODELOOKUP_API_KEY}"
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json()
        products = data.get("products") or []
        if not products:
            return None
        p = products[0]
        return LookupItem(
            name=p.get("product_name") or p.get("title"),
            description=p.get("description") or p.get("category"),
            price=_to_float(p.get("list_price") or p.get("stores", [{}])[0].get("price")),
            currency=None,
            images=p.get("images") or [],
            dimensions=p.get("size"),
            weight=p.get("weight") or p.get("package_weight"),
            source="barcodelookup",
            barcode=barcode,
            url=(p.get("stores") or [{}])[0].get("link"),
        )
    except Exception:
        return None


async def provider_openfoodfacts(barcode: str) -> Optional[LookupItem]:
    if not OPENFOODFACTS_ENABLED:
        return None
    url = f"https://world.openfoodfacts.org/api/v0/product/{barcode}.json"
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json()
        p = data.get("product")
        if not p:
            return None
        images = []
        if p.get("image_url"):
            images.append(p["image_url"])
        return LookupItem(
            name=p.get("product_name"),
            description=p.get("generic_name") or p.get("categories"),
            images=images,
            source="openfoodfacts",
            barcode=barcode,
        )
    except Exception:
        return None


AMAZON_PRICE_RE = re.compile(r"\$\s*([0-9]+[\.,]?[0-9]*)")

async def scrape_amazon(url: str) -> Optional[LookupItem]:
    headers = {"user-agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=headers) as client:
            r = await client.get(url)
        if r.status_code != 200:
            return None
        html = r.text
        # Title
        m = re.search(r'<span id="productTitle"[^>]*>(.*?)</span>', html, re.S)
        title = m.group(1).strip() if m else None
        # Description bullets
        bullets = re.findall(r'id="feature-bullets"[\s\S]*?<li[^>]*>\s*<span[^>]*>(.*?)</span>', html)
        desc = ". ".join([re.sub("<[^>]+>", "", b).strip() for b in bullets][:5]) if bullets else None
        # Price
        pm = AMAZON_PRICE_RE.search(html)
        price = float(pm.group(1).replace(",", ".")) if pm else None
        # Images from data-a-dynamic-image
        dyn = re.search(r'data-a-dynamic-image=\"(\{.*?\})\"', html)
        images: List[str] = []
        if dyn:
            try:
                j = json.loads(dyn.group(1))
                images = list(j.keys())[:6]
            except Exception:
                pass
        return LookupItem(
            name=title,
            description=desc,
            price=price,
            currency="USD" if ".com" in url else "CAD",
            images=images,
            source="Amazon",
            url=url,
        )
    except Exception:
        return None


async def scrape_walmart(url: str) -> Optional[LookupItem]:
    headers = {"user-agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=headers) as client:
            r = await client.get(url)
        if r.status_code != 200:
            return None
        html = r.text
        # Try to parse __NEXT_DATA__
        m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.S)
        if m:
            try:
                j = json.loads(m.group(1))
                data = j.get("props", {}).get("pageProps", {}).get("initialData", {}).get("data", {})
                product = data.get("product", {})
                name = product.get("name")
                short = product.get("shortDescription")
                price = None
                price_obj = data.get("price", {}).get("item", {})
                if isinstance(price_obj, dict):
                    price = _to_float(price_obj.get("price"))
                imgs = [img.get("url") for img in (product.get("imageInfo", {}).get("allImages") or []) if img.get("url")]
                return LookupItem(
                    name=name,
                    description=short,
                    price=price,
                    currency="CAD" if ".ca" in url else "USD",
                    images=imgs[:6],
                    source="Walmart",
                    url=url,
                )
            except Exception:
                pass
        return None
    except Exception:
        return None


async def enrich_from_url(url: str) -> Optional[LookupItem]:
    if "amazon." in url or "a.co" in url or "amzn.to" in url:
        return await scrape_amazon(url)
    if "walmart." in url:
        return await scrape_walmart(url)
    return None


async def resolve_barcode(barcode: str) -> Optional[LookupItem]:
    # Try providers in order
    for fn in (provider_upcitemdb, provider_barcodelookup, provider_openfoodfacts):
        try:
            item = await fn(barcode)  # type: ignore
            if item and (item.name or item.images):
                return item
        except Exception:
            continue
    return None

# -----------------------------
# Endpoints
# -----------------------------
@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": time.time()}


@app.get("/lookup", response_model=LookupItem)
async def lookup(barcode: Optional[str] = Query(default=None), mode: str = Query(default="Catalogue"), url: Optional[str] = Query(default=None), user: User = Depends(get_current_user)):
    if not barcode and not url:
        raise HTTPException(400, "Provide barcode or url")

    item: Optional[LookupItem] = None
    if barcode:
        item = await resolve_barcode(barcode)
    if (not item) and url:
        item = await enrich_from_url(url)

    if not item:
        raise HTTPException(404, "Item not found from providers")

    # Normalize and attach defaults
    if item.currency is None:
        item.currency = "USD"
    if item.source is None:
        item.source = "Internet"
    item_dict = item.dict()
    item_dict["barcode"] = barcode
    item_dict["url"] = url or item.url
    return LookupItem(**item_dict)


@app.post("/airtable/add")
async def airtable_add(req: AirtableAddRequest, user: User = Depends(get_current_user)):
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID):
        raise HTTPException(500, "Airtable not configured")
    if AirtableApi is None:
        raise HTTPException(500, "pyairtable not installed on server")

    mode = req.mode.lower()
    table_map = {
        "bid4more": AIRTABLE_TABLE_BID4MORE,
        "bin4more": AIRTABLE_TABLE_BIN4MORE,
        "pay4more": AIRTABLE_TABLE_PAY4MORE,
        "catalogue": AIRTABLE_TABLE_CATALOGUE,
        "fix4more": AIRTABLE_TABLE_FIX4MORE,
    }
    table_name = table_map.get(mode)
    if not table_name:
        raise HTTPException(400, f"Unsupported mode: {req.mode}")

    api = AirtableApi(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, table_name)

    # Field mapping (adjust to your exact base fields)
    fields: Dict[str, Any] = {
        "Product Name": req.item.name,
        "Description": (req.item.description or "")[:1000],
        "Sale Price": req.item.price,
        "Currency": req.item.currency,
        "Dimensions": req.item.dimensions,
        "Weight": req.item.weight,
        "Scraping Website": req.item.source,
        "Barcode": req.item.barcode,
        "Product URL": req.item.url,
        "Status": "Scraped",
        "Scraping Status": "Added via API",
    }
    if req.item.images:
        fields["Photo Files"] = [{"url": u} for u in req.item.images[:5]]
        fields["Photos"] = ", ".join(req.item.images[:10])

    rec = table.create(fields)
    return {"ok": True, "record_id": rec.get("id")}


# -----------------------------
# Dev server entrypoint
# -----------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
