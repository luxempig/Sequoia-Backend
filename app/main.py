from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings

from app.routers.meta        import router as meta_router
from app.routers.voyages     import router as voyages_router
from app.routers.media       import router as media_router
from app.routers.presidents  import router as presidents_router
from app.routers.people      import router as people_router

s = get_settings()
app = FastAPI(title=s.APP_TITLE)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[str(o) for o in s.CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(meta_router)
app.include_router(voyages_router)
app.include_router(media_router)
app.include_router(presidents_router)
app.include_router(people_router)

@app.get("/", tags=["root"])
def read_root():
    return {"message": "Welcome to the Sequoia API (slug schema)"}
