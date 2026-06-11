import os
import asyncio
import httpx
from app.step_daddy import StepDaddy, Channel
from app.epg import EpgService
from fastapi import Response, status, FastAPI
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from datetime import datetime, timezone
import hmac
import hashlib
import base64
import json
import time
from .utils import urlsafe_base64_decode


fastapi_app = FastAPI()
step_daddy = StepDaddy()
epg = EpgService()
client = httpx.AsyncClient(http2=True, timeout=None, verify=False)
dead_channels: set[str] = set()


@fastapi_app.on_event("startup")
async def _startup_epg_refresh():
    # Non-blocking warm start for EPG/index cache
    try:
        epg.ensure_refresh_async()
    except Exception:
        pass



@fastapi_app.get("/stream/{channel_id}.m3u8")
async def stream(channel_id: str):
    try:
        content = await step_daddy.stream(channel_id)
        dead_channels.discard(str(channel_id))
        return Response(
            content=content,
            media_type="application/vnd.apple.mpegurl",
            headers={f"Content-Disposition": f"attachment; filename={channel_id}.m3u8"}
        )
    except IndexError:
        dead_channels.add(str(channel_id))
        return JSONResponse(content={"error": "Stream not found"}, status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        dead_channels.add(str(channel_id))
        return JSONResponse(content={"error": str(e)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@fastapi_app.get("/key/{url}/{host}")
async def key(url: str, host: str):
    try:
        return Response(
            content=await step_daddy.key(url, host),
            media_type="application/octet-stream",
            headers={"Content-Disposition": "attachment; filename=key"}
        )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@fastapi_app.get("/content/{path}")
async def content(path: str):
    try:
        async def proxy_stream():
            async with client.stream("GET", step_daddy.content_url(path), timeout=60) as response:
                async for chunk in response.aiter_bytes(chunk_size=64 * 1024):
                    yield chunk
        return StreamingResponse(proxy_stream(), media_type="application/octet-stream")
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


async def update_channels():
    while True:
        try:
            await step_daddy.load_channels()
            await asyncio.sleep(300)
        except asyncio.CancelledError:
            continue


def get_channels(include_dead: bool = True):
    channels = []
    for ch in step_daddy.channels:
        match = epg.map_channel_name(ch.name)
        channels.append(Channel(
            id=ch.id,
            name=ch.name,
            tags=ch.tags,
            logo=ch.logo,
            dead=(ch.id in dead_channels),
            tvg_id=match.tvg_id,
            epg_has_data=epg.has_programme_data(match.tvg_id),
        ))
    if include_dead:
        return channels
    return [ch for ch in channels if not ch.dead]


def get_channel(channel_id) -> Channel | None:
    if not channel_id or channel_id == "":
        return None
    channel = next((channel for channel in step_daddy.channels if channel.id == channel_id), None)
    if not channel:
        return None
    match = epg.map_channel_name(channel.name)
    return Channel(
        id=channel.id,
        name=channel.name,
        tags=channel.tags,
        logo=channel.logo,
        dead=(channel.id in dead_channels),
        tvg_id=match.tvg_id,
        epg_has_data=epg.has_programme_data(match.tvg_id),
    )


@fastapi_app.get("/playlist.m3u8")
def playlist():
    enriched_channels = get_channels(include_dead=True)
    return Response(content=step_daddy.playlist(enriched_channels), media_type="application/vnd.apple.mpegurl", headers={"Content-Disposition": "attachment; filename=playlist.m3u8"})


@fastapi_app.get("/channels/status")
def channels_status():
    meta = epg.debug_refresh_meta()
    if meta.get("refreshing"):
        return {
            "dead": sorted(dead_channels),
            "dead_count": len(dead_channels),
            "total_count": len(step_daddy.channels),
            "epg_mapped_count": -1,
            "epg_refreshing": True,
        }
    channels = get_channels(include_dead=True)
    mapped = len([c for c in channels if c.tvg_id])
    return {
        "dead": sorted(dead_channels),
        "dead_count": len(dead_channels),
        "total_count": len(step_daddy.channels),
        "epg_mapped_count": mapped,
        "epg_refreshing": False,
    }


@fastapi_app.get("/epg.xml")
def epg_xml():
    channels = get_channels(include_dead=True)
    tvg_ids = {c.tvg_id for c in channels if c.tvg_id}
    xml = epg.build_filtered_epg_xml(tvg_ids)
    return Response(content=xml, media_type="application/xml")


@fastapi_app.get("/epg/now-next/{channel_id}")
def epg_now_next(channel_id: str):
    ch = get_channel(channel_id)
    if not ch or not ch.tvg_id:
        return {"tvg_id": None, "now": None, "next": None}
    data = epg.get_now_next(ch.tvg_id)
    return {"tvg_id": ch.tvg_id, **data}


@fastapi_app.get("/epg/events")
def epg_events(hours: int = 24, limit: int = 300):
    channels = get_channels(include_dead=True)
    by_tvg = {c.tvg_id: c for c in channels if c.tvg_id}
    events = []
    for e in epg.get_upcoming_events(hours=hours, limit=limit * 2):
        ch = by_tvg.get(e["channel"])
        if not ch:
            continue
        events.append({
            "name": e["title"],
            "time": datetime.fromisoformat(e["start"]).strftime("%H:%M"),
            "dt": e["start"],
            "category": e["category"],
            "channels": [{"name": ch.name, "id": ch.id}],
        })
        if len(events) >= limit:
            break
    return events


async def get_schedule():
    return await step_daddy.schedule()


@fastapi_app.get("/logo/{logo}")
async def logo(logo: str):
    url = urlsafe_base64_decode(logo)
    file = url.split("/")[-1]
    if not os.path.exists("./logo-cache"):
        os.makedirs("./logo-cache")
    if os.path.exists(f"./logo-cache/{file}"):
        return FileResponse(f"./logo-cache/{file}")
    try:
        response = await client.get(url, headers={"user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:137.0) Gecko/20100101 Firefox/137.0"})
        if response.status_code == 200:
            with open(f"./logo-cache/{file}", "wb") as f:
                f.write(response.content)
            return FileResponse(f"./logo-cache/{file}")
        else:
            return JSONResponse(content={"error": "Logo not found"}, status_code=status.HTTP_404_NOT_FOUND)
    except httpx.ConnectTimeout:
        return JSONResponse(content={"error": "Request timed out"}, status_code=status.HTTP_504_GATEWAY_TIMEOUT)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)



@fastapi_app.get("/channels")
def channels(include_dead: bool = False):
    return [c.model_dump() for c in get_channels(include_dead=include_dead)]


# SHARE_TOKEN_V1
SHARE_SECRET = os.environ.get("SHARE_SECRET", "change-me-now")
SHARE_BASE_URL = os.environ.get("SHARE_BASE_URL", os.environ.get("API_URL", "http://127.0.0.1:3000"))

def _b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")

def _b64u_dec(s: str) -> bytes:
    pad = '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)

def _sign(msg: str) -> str:
    return hmac.new(SHARE_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()

def _make_share_token(channel_id: str, ttl_seconds: int = 7200) -> str:
    payload = {"c": str(channel_id), "e": int(time.time()) + int(ttl_seconds)}
    payload_b64 = _b64u(json.dumps(payload, separators=(",", ":")).encode())
    sig = _sign(payload_b64)
    return f"{payload_b64}.{sig}"

def _verify_share_token(token: str):
    try:
        payload_b64, sig = token.split('.', 1)
        if not hmac.compare_digest(_sign(payload_b64), sig):
            return None
        payload = json.loads(_b64u_dec(payload_b64).decode())
        if int(payload.get("e", 0)) < int(time.time()):
            return None
        return payload
    except Exception:
        return None

@fastapi_app.post("/share/create")
def share_create(channel_id: str, ttl_seconds: int = 7200):
    token = _make_share_token(channel_id, ttl_seconds)
    return {
        "token": token,
        "expires_in": ttl_seconds,
        "watch_url": f"{SHARE_BASE_URL}/share/watch/{token}",
        "stream_url": f"{SHARE_BASE_URL}/share/stream/{token}.m3u8",
    }

@fastapi_app.get("/share/stream/{token}.m3u8")
async def share_stream(token: str):
    payload = _verify_share_token(token)
    if not payload:
        return JSONResponse(content={"error": "invalid_or_expired_token"}, status_code=403)
    channel_id = str(payload.get("c", ""))
    try:
        content = await step_daddy.stream(channel_id)
        return Response(content=content, media_type="application/vnd.apple.mpegurl")
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@fastapi_app.get("/share/watch/{token}")
def share_watch(token: str):
    if not _verify_share_token(token):
        return Response(content="Link expired or invalid", media_type="text/plain", status_code=403)
    stream_url = f"/share/stream/{token}.m3u8"
    html = f"""<!doctype html><html><head><meta name='viewport' content='width=device-width,initial-scale=1'><title>Watch Channel</title><script src='https://cdn.jsdelivr.net/npm/hls.js@latest'></script><style>body{{background:#000;color:#fff;font-family:Arial;margin:0}}video{{width:100vw;height:100vh;background:#000}}</style></head><body><video id='v' controls autoplay playsinline></video><script>const v=document.getElementById('v');const u='{stream_url}';if(window.Hls&&Hls.isSupported()){{const h=new Hls();h.loadSource(u);h.attachMedia(v);h.on(Hls.Events.MANIFEST_PARSED,()=>v.play().catch(()=>{{}}));}}else{{v.src=u;v.play().catch(()=>{{}});}}</script></body></html>"""
    return Response(content=html, media_type="text/html")


@fastapi_app.get("/channels/search")
def channels_search(q: str):
    ql = (q or "").lower().strip()
    out = []
    for c in get_channels(include_dead=True):
        if ql in c.name.lower():
            out.append(c.model_dump())
    return out[:50]


@fastapi_app.get("/epg/match/{channel_id}")
def epg_match_debug(channel_id: str):
    ch = get_channel(channel_id)
    if not ch:
        return {"error": "channel_not_found", "channel_id": channel_id}
    m = epg.map_channel_name(ch.name)
    now_next = epg.get_now_next(m.tvg_id) if m.tvg_id else {"now": None, "next": None}
    return {
        "channel_id": ch.id,
        "channel_name": ch.name,
        "mapped_tvg_id": m.tvg_id,
        "confidence": m.confidence,
        "method": m.method,
        "epg_has_data": epg.has_programme_data(m.tvg_id),
        "now": now_next.get("now"),
        "next": now_next.get("next"),
    }


@fastapi_app.get("/epg/debug/refresh")
def epg_debug_refresh():
    meta = epg.debug_refresh_meta()
    return {
        "epg_ready": epg._epg_ready,
        "last_refresh": epg._last_refresh,
        "meta": meta,
    }
