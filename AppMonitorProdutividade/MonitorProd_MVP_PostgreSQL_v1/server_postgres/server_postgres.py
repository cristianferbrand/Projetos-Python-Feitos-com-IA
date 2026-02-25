# server_postgres.py
import base64
import os
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Depends, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import (create_engine, Column, Integer, String, DateTime, Float,
                        ForeignKey, desc, Index, func)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, Session
from dotenv import load_dotenv

load_dotenv()

TZ = os.getenv("TZ", "America/Sao_Paulo")
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")
DATABASE_URL = os.getenv("DATABASE_URL")  # obrigatório em produção
MEDIA_DIR = os.getenv("MEDIA_DIR", "media")

if not DATABASE_URL or not DATABASE_URL.startswith("postgresql+"):
    raise RuntimeError("Defina DATABASE_URL para PostgreSQL (postgresql+psycopg2://...)")

os.makedirs(MEDIA_DIR, exist_ok=True)

engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_size=10,
    max_overflow=10,
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()

# === Models ===
class Device(Base):
    __tablename__ = "devices"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    token = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_seen = Column(DateTime(timezone=True), server_default=func.now())
    events = relationship("Event", back_populates="device", cascade="all,delete-orphan")
    screenshots = relationship("Screenshot", back_populates="device", cascade="all,delete-orphan")

    __table_args__ = (
        Index("ix_device_last_seen", "last_seen"),
    )

class Event(Base):
    __tablename__ = "events"
    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey("devices.id", ondelete="CASCADE"))
    process_name = Column(String, nullable=False)
    window_title = Column(String, nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False)
    duration_sec = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    device = relationship("Device", back_populates="events")

    __table_args__ = (
        Index("ix_events_device_started", "device_id", "started_at"),
        Index("ix_events_device_created", "device_id", "created_at"),
    )

class Screenshot(Base):
    __tablename__ = "screenshots"
    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey("devices.id", ondelete="CASCADE"))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    file_path = Column(String, nullable=False)
    device = relationship("Device", back_populates="screenshots")

    __table_args__ = (
        Index("ix_shots_device_created", "device_id", "created_at"),
    )

Base.metadata.create_all(bind=engine)

app = FastAPI(title="MonitorProd Server (PostgreSQL)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/media", StaticFiles(directory=MEDIA_DIR), name="media")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# === Schemas ===
class HeartbeatIn(BaseModel):
    name: str

class EventIn(BaseModel):
    process_name: str
    window_title: str
    started_at: datetime
    duration_sec: float

class EventsIn(BaseModel):
    events: list[EventIn]

class ScreenshotIn(BaseModel):
    image_b64: str  # base64 JPEG/PNG

# === Helpers ===
def _require_device(db: Session, token: str, name_hint: str | None = None) -> Device:
    if not token:
        raise HTTPException(status_code=401, detail="Missing X-Token header")
    dev = db.query(Device).filter(Device.token == token).one_or_none()
    if dev:
        return dev
    if not name_hint:
        raise HTTPException(status_code=401, detail="Unknown device token and no name provided")
    dev = Device(name=name_hint, token=token)
    db.add(dev)
    db.commit()
    db.refresh(dev)
    return dev

# === Agent endpoints ===
@app.post("/api/agent/heartbeat")
def heartbeat(payload: HeartbeatIn, x_token: str = Header(default=""), db: Session = Depends(get_db)):
    tz = ZoneInfo(TZ)
    dev = _require_device(db, x_token, name_hint=payload.name)
    dev.last_seen = datetime.now(tz)
    db.commit()
    return {"ok": True, "device_id": dev.id, "now": dev.last_seen.isoformat()}

@app.post("/api/agent/events")
def post_events(payload: EventsIn, x_token: str = Header(default=""), db: Session = Depends(get_db)):
    tz = ZoneInfo(TZ)
    now = datetime.now(tz)
    dev = _require_device(db, x_token)
    for e in payload.events:
        if e.duration_sec <= 0 or e.duration_sec > 8 * 3600:
            continue
        evt = Event(
            device_id=dev.id,
            process_name=e.process_name[:255],
            window_title=e.window_title[:255],
            started_at=e.started_at if e.started_at.tzinfo else e.started_at.replace(tzinfo=tz),
            duration_sec=float(e.duration_sec),
            created_at=now,
        )
        db.add(evt)
    dev.last_seen = now
    db.commit()
    return {"ok": True}

@app.post("/api/agent/screenshot")
def post_screenshot(payload: ScreenshotIn, x_token: str = Header(default=""), db: Session = Depends(get_db)):
    tz = ZoneInfo(TZ)
    dev = _require_device(db, x_token)
    img_bytes = base64.b64decode(payload.image_b64)
    ts = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
    device_dir = os.path.join(MEDIA_DIR, f"device_{dev.id}")
    os.makedirs(device_dir, exist_ok=True)
    fname = f"{ts}_{uuid.uuid4().hex[:6]}.jpg"
    fpath = os.path.join(device_dir, fname)
    with open(fpath, "wb") as f:
        f.write(img_bytes)
    shot = Screenshot(device_id=dev.id, file_path=f"/media/device_{dev.id}/{fname}")
    dev.last_seen = datetime.now(tz)
    db.add(shot)
    db.commit()
    return {"ok": True, "url": shot.file_path}

# === Dashboard endpoints ===
@app.get("/api/devices")
def list_devices(db: Session = Depends(get_db)):
    tz = ZoneInfo(TZ)
    now = datetime.now(tz)
    devices = db.query(Device).all()
    data = []
    for d in devices:
        last_evt = (
            db.query(Event)
            .filter(Event.device_id == d.id)
            .order_by(desc(Event.created_at))
            .first()
        )
        online = d.last_seen and (now - d.last_seen) <= timedelta(seconds=60)
        data.append({
            "id": d.id,
            "name": d.name,
            "online": bool(online),
            "last_seen": d.last_seen.isoformat() if d.last_seen else None,
            "current_window": last_evt.window_title if last_evt else "—",
            "current_process": last_evt.process_name if last_evt else "—",
        })
    return {"devices": data}

@app.get("/api/device/{device_id}/summary/today")
def summary_today(device_id: int, db: Session = Depends(get_db)):
    tz = ZoneInfo(TZ)
    now = datetime.now(tz)
    start = datetime(now.year, now.month, now.day, tzinfo=tz)
    rows = (
        db.query(Event.process_name, Event.window_title, Event.duration_sec)
        .filter(Event.device_id == device_id, Event.started_at >= start)
        .all()
    )
    by_proc: dict[str, float] = {}
    for p, _w, s in rows:
        by_proc[p] = by_proc.get(p, 0.0) + float(s)
    top = sorted(by_proc.items(), key=lambda kv: kv[1], reverse=True)[:10]
    return {
        "top_process_minutes": [
            {"process_name": k, "minutes": round(v / 60.0, 1)} for k, v in top
        ]
    }

@app.get("/api/device/{device_id}/last_screenshot")
def last_shot(device_id: int, db: Session = Depends(get_db)):
    shot = (
        db.query(Screenshot)
        .filter(Screenshot.device_id == device_id)
        .order_by(desc(Screenshot.created_at))
        .first()
    )
    if not shot:
        return {"url": None}
    return {"url": shot.file_path, "created_at": shot.created_at.isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server_postgres:app", host="0.0.0.0", port=8000, reload=False)
