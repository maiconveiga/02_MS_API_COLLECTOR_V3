# app/main.py
from fastapi import FastAPI, HTTPException, Query, Path as ApiPath, Body
from pydantic import BaseModel, HttpUrl, Field, constr
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import aiosqlite, httpx, os, uuid, asyncio
from fastapi.middleware.cors import CORSMiddleware
import re

# ---------------------------------------------------------
# App + CORS
# ---------------------------------------------------------
app = FastAPI(title="API_COLLECTOR_V3", version="0.8.1")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ---------------------------------------------------------
# DB (usado para comments; collect não persiste)
# ---------------------------------------------------------
DB_DIR = Path("data")
DB_PATH = DB_DIR / "collector.db"

CREATE_ALARMS = """
CREATE TABLE IF NOT EXISTS alarms(
  id TEXT PRIMARY KEY,
  itemReference TEXT,
  name TEXT,
  message TEXT,
  isAckRequired INTEGER,
  type TEXT,
  priority INTEGER,
  triggerValue_value TEXT,
  triggerValue_units TEXT,
  creationTime TEXT,
  isAcknowledged INTEGER,
  isDiscarded INTEGER,
  category TEXT,
  objectUrl TEXT,
  annotationsUrl TEXT,
  source_base_url TEXT,
  inserted_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""
CREATE_COMMENTS = """
CREATE TABLE IF NOT EXISTS comments(
  id TEXT PRIMARY KEY,
  alarm_id TEXT NOT NULL,
  text TEXT NOT NULL CHECK(length(text)<=255),
  status TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(alarm_id) REFERENCES alarms(id) ON DELETE CASCADE
);
"""
CREATE_IDX = "CREATE INDEX IF NOT EXISTS idx_comments_alarm_id ON comments(alarm_id);"

_clients: dict[bool, httpx.AsyncClient] = {}

@asynccontextmanager
async def lifespan(_: FastAPI):
    os.makedirs(DB_DIR, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA foreign_keys=ON;")
        await db.execute(CREATE_ALARMS)
        await db.execute(CREATE_COMMENTS)
        await db.execute(CREATE_IDX)
        await db.commit()
    _clients[True]  = httpx.AsyncClient(timeout=20.0, follow_redirects=True, verify=True)
    _clients[False] = httpx.AsyncClient(timeout=20.0, follow_redirects=True, verify=False)
    try:
        yield
    finally:
        await asyncio.gather(*[c.aclose() for c in _clients.values() if not c.is_closed])

app.router.lifespan_context = lifespan

def _client(verify_ssl: bool) -> httpx.AsyncClient:
    return _clients[bool(verify_ssl)]

# ---------------------------------------------------------
# Schemas
# ---------------------------------------------------------
class LoginIn(BaseModel):
    base_url: HttpUrl
    usuario: str
    senha: str
    verify_ssl: bool = False

class APIInput(BaseModel):
    base_url: HttpUrl
    token: str
    pageSize: int = Field(100, ge=1, le=10000)
    page: int = Field(1, ge=1, le=10)
    offset: int = Field(0, ge=-12, le=12)
    verify_ssl: bool = False

class BatchRequest(BaseModel):
    apis: List[APIInput] = Field(..., min_items=1)

class CommentCreate(BaseModel):
    alarm_id: str
    text: constr(min_length=1, max_length=255)
    status: str
    offset: int = Field(0, ge=-12, le=12)

class CommentUpdate(BaseModel):
    text: Optional[constr(min_length=1, max_length=255)] = None
    status: Optional[str] = None

class CommentOut(BaseModel):
    id: str
    alarm_id: str
    text: str
    status: Optional[str]
    created_at: str

# ---------------------------------------------------------
# Mapeamentos p/ exibição
# ---------------------------------------------------------
UNITS_MAP: Dict[Optional[str], str] = {
    None: "", "": "",
    "unitEnumSet.milligrams": "mg",
    "unitEnumSet.millimetersPerSecond": "mm/s",
    "unitEnumSet.degC": "°C",
    "unitEnumSet.degF": "°F",
    "unitEnumSet.kilopascals": "kPa",
    "unitEnumSet.kilowatts": "kW",
    "unitEnumSet.metersPerSecondPerSecond": "m/s²",
    "unitEnumSet.perHour": "/h",
    "unitEnumSet.perMinute": "/m",
    "unitEnumSet.percent": "%",
    "unitEnumSet.noUnits": "",
}

VALUE_MAP: Dict[str, str] = {
    # alguns exemplos comuns; mantém fallback p/ outros
    "offonEnumSet.1": "On",
    "offonEnumSet.0": "Off",
    "yesNoEnumSet.1": "Yes",
    "yesNoEnumSet.0": "No",
    "noyesEnumSet.1": "No",
    "noyesEnumSet.0": "Yes",
    "falsetrueEnumSet.falsetrueTrue": "True",
    "falsetrueEnumSet.falsetrueFalse": "False",
    "controllerStatusEnumSet.csOnline": "Online",
    "controllerStatusEnumSet.csOffline": "Offline",
}

TYPE_MAP: Dict[str, str] = {
    "alarmValueEnumSet.avAlarm": "Alarm",
    "alarmValueEnumSet.avHiAlarm": "High Alarm",
    "alarmValueEnumSet.avLoAlarm": "Low Alarm",
    "alarmValueEnumSet.avNormal": "Normal",
    "alarmValueEnumSet.avOffline": "Offline",
    "alarmValueEnumSet.avOnline": "Online",
    "alarmValueEnumSet.avUnreliable": "Unreliable",
}

# ---------------------------------------------------------
# Utils
# ---------------------------------------------------------
def _safe_json(r: httpx.Response):
    try:
        return r.json()
    except Exception:
        return {}

def _extract_items(external: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(external, dict):
        return []
    return external.get("items") or (external.get("data") or {}).get("items") or []

def _now(off: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=off)).strftime("%d/%m/%y %H:%M:%S")

_qstr = re.compile(r'^(?P<q>["\'])(?P<inner>.*)(?P=q)$')

def _clean_value(v: Any) -> str:
    """Normaliza o valor para exibição."""
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, bool):
        return "True" if v else "False"
    s = str(v).strip()
    # mapeia enums comuns (on/off/yes/no etc.)
    if s in VALUE_MAP:
        return VALUE_MAP[s]
    # remove aspas redundantes: "\"68.9\"" ou "'abc'"
    m = _qstr.match(s)
    if m:
        s = m.group("inner")
    return s

def _clean_units(u: Optional[str]) -> str:
    return UNITS_MAP.get(u, u or "")

def _map_type(t: Optional[str]) -> str:
    if not t:
        return ""
    return TYPE_MAP.get(t, t)

def _decorate_item_for_display(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mantém TODO o payload original e adiciona:
      - triggerValue_value / triggerValue_units "bonitos"
      - type_raw (original) e type (legível)
    """
    out = dict(raw)  # cópia p/ não mutar o original
    tv = raw.get("triggerValue") if isinstance(raw.get("triggerValue"), dict) else {}
    v = _clean_value(tv.get("value"))
    u = _clean_units(tv.get("units"))
    out["triggerValue_value"] = v
    out["triggerValue_units"] = u
    t_raw = raw.get("type")
    out["type_raw"] = t_raw
    out["type"] = _map_type(t_raw)
    return out

def _decorate_list(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [_decorate_item_for_display(x) for x in items]

# ---------------------------------------------------------
# Auth (pass-through)
# ---------------------------------------------------------
@app.post("/auth/login")
async def auth_login(payload: LoginIn):
    url = f"{str(payload.base_url).rstrip('/')}/login"
    body = {"username": payload.usuario, "password": payload.senha}
    headers = {"accept": "application/json", "content-type": "application/json"}
    try:
        resp = await _client(payload.verify_ssl).post(url, json=body, headers=headers, timeout=15.0)
        resp.raise_for_status()
        return _safe_json(resp)
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail={"upstream_error": _safe_json(e.response)})
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail={"error": f"Falha ao conectar em {url}", "detail": str(e)})

# ---------------------------------------------------------
# Collect (GET - NÃO persiste; passa cru + campos tratados p/ UI)
# ---------------------------------------------------------
@app.get("/collect/alarms")
async def get_alarms(
    base_url: HttpUrl,
    token: str,
    pageSize: int = Query(100, ge=1, le=10000),
    page: int = Query(1, ge=1, le=10),
    offset: int = Query(0, ge=-12, le=12),
    verify_ssl: bool = False,
):
    url = f"{str(base_url).rstrip('/')}/alarms"
    headers = {"accept": "application/json", "authorization": f"Bearer {token}"}
    params = {"pageSize": pageSize, "page": page}
    try:
        resp = await _client(verify_ssl).get(url, headers=headers, params=params)
        resp.raise_for_status()
        external = _safe_json(resp)
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail={"upstream_error": _safe_json(e.response)})
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail={"error": f"Falha ao conectar em {url}", "detail": str(e)})

    items_raw = _extract_items(external)
    items_ui  = _decorate_list(items_raw)

    return {
        "page": page,
        "pageSize": pageSize,
        "offsetAppliedHours": offset,
        "count_items": len(items_raw),
        "items": items_ui,  # mantém tudo + fields tratados
    }

# ---------------------------------------------------------
# Collect (POST - batch; NÃO persiste; idem tratamento)
# ---------------------------------------------------------
async def _fetch_one(api: APIInput) -> Dict[str, Any]:
    url = f"{str(api.base_url).rstrip('/')}/alarms"
    headers = {"accept": "application/json", "authorization": f"Bearer {api.token}"}
    params = {"pageSize": api.pageSize, "page": api.page}
    try:
        resp = await _client(api.verify_ssl).get(url, headers=headers, params=params)
        resp.raise_for_status()
        external = _safe_json(resp)
    except httpx.HTTPStatusError as e:
        return {"ok": False, "base_url": str(api.base_url), "error": {"status_code": e.response.status_code, "upstream_error": _safe_json(e.response)}}
    except httpx.RequestError as e:
        return {"ok": False, "base_url": str(api.base_url), "error": {"status_code": 502, "message": f"Falha ao conectar em {url}", "detail": str(e)}}

    items_raw = _extract_items(external)
    items_ui  = _decorate_list(items_raw)

    return {
        "ok": True,
        "base_url": str(api.base_url),
        "page": api.page,
        "pageSize": api.pageSize,
        "offsetAppliedHours": api.offset,
        "count_items": len(items_raw),
        "items": items_ui,
    }

@app.post("/collect/alarms")
async def collect_alarms_list(payload: BatchRequest = Body(...)):
    results = await asyncio.gather(*(_fetch_one(api) for api in payload.apis))
    ok   = [r for r in results if r.get("ok")]
    fail = [r for r in results if not r.get("ok")]

    total_items = sum(r["count_items"] for r in ok)
    flat: List[Dict[str, Any]] = [it for r in ok for it in r["items"]]

    return {
        "total_apis": len(payload.apis),
        "succeeded": len(ok),
        "failed": len(fail),
        "total_items": total_items,
        "errors": fail,
        "by_api": [
            {
                "base_url": r["base_url"],
                "page": r["page"],
                "pageSize": r["pageSize"],
                "offsetAppliedHours": r["offsetAppliedHours"],
                "count_items": r["count_items"],
                "items": r["items"],
            }
            for r in ok
        ],
        "items": flat,
    }

# ---------------------------------------------------------
# Comments (igual)
# ---------------------------------------------------------
class _CommentSQL:
    ins = "INSERT INTO comments (id, alarm_id, text, status, created_at) VALUES (?,?,?,?,?)"
    by_id = "SELECT id, alarm_id, text, status, created_at FROM comments WHERE id=?"
    list = """
    SELECT id, alarm_id, text, status, created_at
    FROM comments
    WHERE (? IS NULL OR alarm_id = ?)
    ORDER BY created_at DESC
    LIMIT ? OFFSET ?
    """
    upd = "UPDATE comments SET text=?, status=? WHERE id=?"
    del_ = "DELETE FROM comments WHERE id=?"

async def _alarm_exists(alarm_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM alarms WHERE id=? LIMIT 1", (alarm_id,))
        row = await cur.fetchone(); await cur.close()
    return bool(row)

@app.post("/comments", response_model=CommentOut)
async def create_comment(payload: CommentCreate):
    # Observação: se os alarmes não foram persistidos, isso pode retornar 404.
    if not await _alarm_exists(payload.alarm_id):
        raise HTTPException(status_code=404, detail="Alarme não encontrado")
    cid = str(uuid.uuid4()); created = _now(payload.offset)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys=ON;")
        try:
            await db.execute(_CommentSQL.ins, (cid, payload.alarm_id, payload.text, payload.status, created))
            await db.commit()
        except aiosqlite.IntegrityError:
            raise HTTPException(status_code=400, detail="Falha ao salvar comentário")
    return CommentOut(id=cid, alarm_id=payload.alarm_id, text=payload.text, status=payload.status, created_at=created)

@app.get("/comments", response_model=List[CommentOut])
async def list_comments(alarm_id: Optional[str] = None, pageSize: int = Query(50, ge=1, le=1000), page: int = Query(1, ge=1)):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(_CommentSQL.list, (alarm_id, alarm_id, pageSize, (page-1)*pageSize))
        rows = await cur.fetchall(); await cur.close()
    return [CommentOut(id=r[0], alarm_id=r[1], text=r[2], status=r[3], created_at=r[4]) for r in rows]

@app.get("/comments/{comment_id}", response_model=CommentOut)
async def get_comment(comment_id: str = ApiPath(...)):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(_CommentSQL.by_id, (comment_id,))
        row = await cur.fetchone(); await cur.close()
    if not row: raise HTTPException(status_code=404, detail="Comentário não encontrado")
    return CommentOut(id=row[0], alarm_id=row[1], text=row[2], status=row[3], created_at=row[4])

@app.patch("/comments/{comment_id}", response_model=CommentOut)
async def update_comment(comment_id: str, payload: CommentUpdate):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(_CommentSQL.by_id, (comment_id,))
        row = await cur.fetchone()
        if not row:
            await cur.close()
            raise HTTPException(status_code=404, detail="Comentário não encontrado")
        new_text = payload.text if payload.text is not None else row[2]
        new_status = payload.status if payload.status is not None else row[3]
        await db.execute(_CommentSQL.upd, (new_text, new_status, comment_id))
        await db.commit(); await cur.close()
    return CommentOut(id=comment_id, alarm_id=row[1], text=new_text, status=new_status, created_at=row[4])

@app.delete("/comments/{comment_id}")
async def delete_comment(comment_id: str = ApiPath(...)):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(_CommentSQL.del_, (comment_id,)); await db.commit()
        deleted = cur.rowcount; await cur.close()
    if not deleted: raise HTTPException(status_code=404, detail="Comentário não encontrado")
    return {"deleted": True, "id": comment_id}

# ---------------------------------------------------------
# DB utilities
# ---------------------------------------------------------
@app.delete("/db/alarms/clear")
async def clear_alarms(vacuum: bool = Query(False)):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys=ON;")
        cur = await db.execute("SELECT COUNT(*) FROM alarms"); (n,) = await cur.fetchone(); await cur.close()
        await db.execute("DELETE FROM alarms"); await db.commit()
        if vacuum:
            await db.execute("VACUUM"); await db.commit()
    return {"table": "alarms", "deleted": n, "vacuum": vacuum}

@app.delete("/db/comments/clear")
async def clear_comments(vacuum: bool = Query(False)):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys=ON;")
        cur = await db.execute("SELECT COUNT(*) FROM comments"); (n,) = await cur.fetchone(); await cur.close()
        await db.execute("DELETE FROM comments"); await db.commit()
        if vacuum:
            await db.execute("VACUUM"); await db.commit()
    return {"table": "comments", "deleted": n, "vacuum": vacuum}

@app.delete("/db/alarms/without-comments")
async def delete_alarms_without_comments(dry_run: bool = Query(False)):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys=ON;")
        cur = await db.execute("""
          SELECT COUNT(*) FROM alarms a
          WHERE NOT EXISTS (SELECT 1 FROM comments c WHERE c.alarm_id=a.id)
        """); (to_delete,) = await cur.fetchone(); await cur.close()
        if dry_run:
            return {"dry_run": True, "would_delete": to_delete}
        await db.execute("""
          DELETE FROM alarms
          WHERE id IN (
            SELECT a.id FROM alarms a
            LEFT JOIN comments c ON c.alarm_id=a.id
            WHERE c.alarm_id IS NULL
          )
        """); await db.commit()
    return {"dry_run": False, "deleted": to_delete}
