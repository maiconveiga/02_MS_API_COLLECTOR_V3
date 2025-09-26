from fastapi import FastAPI, HTTPException, Query, Path as ApiPath, Body
from pydantic import BaseModel, HttpUrl, Field, constr
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import aiosqlite, httpx, os, uuid, asyncio

app = FastAPI(title="API_COLLECTOR_V3", version="0.7.1")

DB_DIR = Path("data")
DB_PATH = DB_DIR / "collector.db"

UNITS_MAP = {None:"-","":"-","unitEnumSet.milligrams":"mg","unitEnumSet.millimetersPerSecond":"mm/s","unitEnumSet.degC":"°C","unitEnumSet.kilopascals":"kPa","unitEnumSet.noUnits":"'","unitEnumSet.kilowatts":"kW","unitEnumSet.jaKilogramsPerSqCm":"kg/m³","unitEnumSet.metersPerSecondPerSecond":"m/s²","unitEnumSet.perHour":"/h","unitEnumSet.perMinute":"/m","unitEnumSet.percent":"%","unitEnumSet.degF":"°F"}
VALUE_MAP = {"yesNoEnumSet.1":"Yes","yesNoEnumSet.0":"No","sabControllerStatusEnumSet.sabCsOffline":"Offline","offonEnumSet.1":"On","offonEnumSet.0":"Off","offAutoEnumSet.1":"Auto","offAutoEnumSet.0":"Manual","objectStatusEnumSet.osHighAlarm":"High Alarm","objectStatusEnumSet.osAlarm":"Alarm","noyesEnumSet.1":"No","noyesEnumSet.0":"Yes","normalAlarmEnumSet.naNormal":"Normal","normalAlarmEnumSet.naAlarm":"Alarm","normalAlarm2EnumSet.na2Normal":"Normal","normalAlarm2EnumSet.na2Alarm":"Alarm","localremoteEnumSet.0":"Local","localremoteEnumSet.1":"Remote","jciSystemStatusEnumSet.startupInProgress":"Startup in progress","jciSystemStatusEnumSet.onboardUploadInProgress":"Onboard upload in progress","jciSystemStatusEnumSet.jciOperational":"Operational","jciSystemStatusEnumSet.instanceUploadInProgress":"Instance upload in progress","falsetrueEnumSet.falsetrueTrue":"True","falsetrueEnumSet.falsetrueFalse":"False","Expired":"Experied","controllerStatusEnumSet.csOnline":"Online","controllerStatusEnumSet.csOffline":"Offline","binarypvEnumSet.bacbinInactive":"Inactive","binarypvEnumSet.bacbinActive":"Active","batteryConditionEnumSet.bcBatteryService":"Battery in service","batteryConditionEnumSet.bcBatteryGood":"Battery good","batteryConditionEnumSet.bcBatteryDefective":"Battery defective"}
TYPE_MAP = {"alarmValueEnumSet.avAlarm":"Alarm","alarmValueEnumSet.avNormal":"Normal","alarmValueEnumSet.avHiAlarm":"High Alarm","alarmValueEnumSet.avLoAlarm":"Low Alarm","unitEnumSet.degC":"°C","alarmValueEnumSet.avOnline":"Online","alarmValueEnumSet.avOffline":"Offline","alarmValueEnumSet.avUnreliable":"Unreliable","alarmValueEnumSet.avFault":"Fault","alarmValueEnumSet.avHiWarn":"High Warn","alarmValueEnumSet.avLoWarn":"Low Warn"}

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

def _u(u: Optional[str]) -> str: return UNITS_MAP.get(u, u if u else "-")
def _v(v: Any) -> str: return str(v) if isinstance(v,(int,float)) else VALUE_MAP.get(v, v or "")
def _t(t: Optional[str]) -> str: return TYPE_MAP.get(t, t or "")
def _now(off:int) -> str: return (datetime.now(timezone.utc)+timedelta(hours=off)).strftime("%d/%m/%y %H:%M:%S")
def _adj(iso: Optional[str], off:int) -> Optional[str]:
    if not iso: return None
    try:
        if iso.endswith("Z"):
            dt = datetime.strptime(iso,"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(iso.replace("Z","+00:00"))
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return (dt+timedelta(hours=off)).strftime("%d/%m/%y %H:%M:%S")
    except Exception:
        return iso

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
    return _clients[not not verify_ssl]

async def _save_alarms(items: List[Dict[str, Any]], source: str, off: int) -> int:
    if not items: return 0
    now = _now(off)
    rows = []
    for it in items:
        trig = it.get("triggerValue") or {}
        rows.append((
            it.get("id"), it.get("itemReference"), it.get("name"), it.get("message"),
            1 if it.get("isAckRequired") else 0, _t(it.get("type")), it.get("priority"),
            _v(trig.get("value") if isinstance(trig,dict) else None),
            _u(trig.get("units") if isinstance(trig,dict) else None),
            _adj(it.get("creationTime"), off),
            1 if it.get("isAcknowledged") else 0, 1 if it.get("isDiscarded") else 0,
            it.get("category"), it.get("objectUrl"), it.get("annotationsUrl"),
            source, now, now
        ))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys=ON;")
        await db.executemany("""
            INSERT INTO alarms(
              id,itemReference,name,message,isAckRequired,type,priority,
              triggerValue_value,triggerValue_units,creationTime,isAcknowledged,isDiscarded,
              category,objectUrl,annotationsUrl,source_base_url,inserted_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
              itemReference=excluded.itemReference,
              name=excluded.name,
              message=excluded.message,
              isAckRequired=excluded.isAckRequired,
              type=excluded.type,
              priority=excluded.priority,
              triggerValue_value=excluded.triggerValue_value,
              triggerValue_units=excluded.triggerValue_units,
              creationTime=excluded.creationTime,
              isAcknowledged=excluded.isAcknowledged,
              isDiscarded=excluded.isDiscarded,
              category=excluded.category,
              objectUrl=excluded.objectUrl,
              annotationsUrl=excluded.annotationsUrl,
              source_base_url=excluded.source_base_url,
              updated_at=excluded.updated_at;
        """, rows)
        await db.commit()
    return len(rows)

def _safe_json(r: httpx.Response):
    try: return r.json()
    except Exception: return {"error": r.text}

@app.post("/auth/login")
async def auth_login(payload: LoginIn):
    url = f"{str(payload.base_url).rstrip('/')}/login"
    body = {"username": payload.usuario, "password": payload.senha}
    headers = {"accept":"application/json","content-type":"application/json"}
    try:
        resp = await _client(payload.verify_ssl).post(url, json=body, headers=headers, timeout=15.0)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail={"upstream_error": _safe_json(e.response)})
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail={"error": f"Falha ao conectar em {url}", "detail": str(e)})

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
    headers = {"accept":"application/json","authorization":f"Bearer {token}"}
    params = {"pageSize":pageSize,"page":page}
    try:
        resp = await _client(verify_ssl).get(url, headers=headers, params=params)
        resp.raise_for_status()
        external = resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail={"upstream_error": _safe_json(e.response)})
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail={"error": f"Falha ao conectar em {url}", "detail": str(e)})
    items = (external or {}).get("items") or ((external or {}).get("data") or {}).get("items") or []
    saved = await _save_alarms(items, str(base_url), offset)
    return {"page": page, "pageSize": pageSize, "offsetAppliedHours": offset, "saved": saved, "data": external}

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
    if not await _alarm_exists(payload.alarm_id): raise HTTPException(status_code=404, detail="Alarme não encontrado")
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

@app.get("/db/alarms/grouped")
async def get_alarms_grouped(
    item_ref: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    limit_rows: int = Query(10000, ge=1, le=100000),
    offset_rows: int = Query(0, ge=0),
):
    sql = """
      SELECT itemReference,name,triggerValue_value,triggerValue_units,creationTime,isAcknowledged,isDiscarded,type
      FROM alarms
      WHERE (? IS NULL OR itemReference LIKE ?)
        AND (? IS NULL OR name LIKE ?)
      ORDER BY itemReference,name,creationTime DESC
      LIMIT ? OFFSET ?
    """
    like_item = f"%{item_ref}%" if item_ref else None
    like_name = f"%{name}%" if name else None
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(sql, (item_ref, like_item, name, like_name, limit_rows, offset_rows))
        rows = await cur.fetchall(); await cur.close()
    g: Dict[Tuple[str,str], List[Dict[str,Any]]] = {}
    for r in rows:
        g.setdefault((r[0],r[1]), []).append({
            "triggerValue_value": r[2], "triggerValue_units": r[3], "creationTime": r[4],
            "isAcknowledged": bool(r[5]), "isDiscarded": bool(r[6]), "type": r[7]
        })
    out = [{"itemReference":k[0], "name":k[1], "count_items":len(v), "items":v} for k,v in g.items()]
    return {"count_groups": len(out), "groups": out}

@app.delete("/db/alarms/clear")
async def clear_alarms(vacuum: bool = Query(False)):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys=ON;")
        cur = await db.execute("SELECT COUNT(*) FROM alarms"); (n,) = await cur.fetchone(); await cur.close()
        await db.execute("DELETE FROM alarms"); await db.commit()
        if vacuum: await db.execute("VACUUM"); await db.commit()
    return {"table": "alarms", "deleted": n, "vacuum": vacuum}

@app.delete("/db/comments/clear")
async def clear_comments(vacuum: bool = Query(False)):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys=ON;")
        cur = await db.execute("SELECT COUNT(*) FROM comments"); (n,) = await cur.fetchone(); await cur.close()
        await db.execute("DELETE FROM comments"); await db.commit()
        if vacuum: await db.execute("VACUUM"); await db.commit()
    return {"table": "comments", "deleted": n, "vacuum": vacuum}

@app.delete("/db/alarms/without-comments")
async def delete_alarms_without_comments(dry_run: bool = Query(False)):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys=ON;")
        cur = await db.execute("""
          SELECT COUNT(*) FROM alarms a
          WHERE NOT EXISTS (SELECT 1 FROM comments c WHERE c.alarm_id=a.id)
        """); (to_delete,) = await cur.fetchone(); await cur.close()
        if dry_run: return {"dry_run": True, "would_delete": to_delete}
        await db.execute("""
          DELETE FROM alarms
          WHERE id IN (
            SELECT a.id FROM alarms a
            LEFT JOIN comments c ON c.alarm_id=a.id
            WHERE c.alarm_id IS NULL
          )
        """); await db.commit()
    return {"dry_run": False, "deleted": to_delete}

async def _fetch_one(api: APIInput) -> Dict[str, Any]:
    url = f"{str(api.base_url).rstrip('/')}/alarms"
    headers = {"accept":"application/json","authorization":f"Bearer {api.token}"}
    params = {"pageSize": api.pageSize, "page": api.page}
    try:
        resp = await _client(api.verify_ssl).get(url, headers=headers, params=params)
        resp.raise_for_status()
        external = resp.json()
    except httpx.HTTPStatusError as e:
        return {"ok": False, "base_url": str(api.base_url), "error": {"status_code": e.response.status_code, "upstream_error": _safe_json(e.response)}}
    except httpx.RequestError as e:
        return {"ok": False, "base_url": str(api.base_url), "error": {"status_code": 502, "message": f"Falha ao conectar em {url}", "detail": str(e)}}
    items = (external or {}).get("items") or ((external or {}).get("data") or {}).get("items") or []
    saved = await _save_alarms(items, str(api.base_url), api.offset)
    return {"ok": True, "base_url": str(api.base_url), "page": api.page, "pageSize": api.pageSize, "offsetAppliedHours": api.offset, "count_items": len(items), "saved": saved, "data": external, "items": items}

@app.post("/collect/alarms")
async def collect_alarms_list(payload: BatchRequest = Body(...)):
    results = await asyncio.gather(*(_fetch_one(api) for api in payload.apis))
    ok = [r for r in results if r.get("ok")]
    fail = [r for r in results if not r.get("ok")]
    total_items = sum(r["count_items"] for r in ok)
    total_saved = sum(r["saved"] for r in ok)
    flat = []
    for r in ok:
        src = r["base_url"]
        flat.extend({**it, "__source_base_url": src} for it in r["items"])
    return {"total_apis": len(payload.apis), "succeeded": len(ok), "failed": len(fail), "total_items": total_items, "total_saved": total_saved, "errors": fail, "by_api": ok, "items": flat}
