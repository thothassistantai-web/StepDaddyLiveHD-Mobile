import asyncio
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from app.backend import fastapi_app, step_daddy, update_channels

@fastapi_app.on_event("startup")
async def _startup():
    try:
        await step_daddy.load_channels()
    except Exception:
        pass
    fastapi_app.state._channels_task = asyncio.create_task(update_channels())

@fastapi_app.on_event("shutdown")
async def _shutdown():
    t = getattr(fastapi_app.state, "_channels_task", None)
    if t:
        t.cancel()

fastapi_app.mount("/ui", StaticFiles(directory="webui", html=True), name="ui")

@fastapi_app.get("/")
async def root_ui():
    return FileResponse("webui/index.html")
