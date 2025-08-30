
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.voyages     import router as voyages_router
from app.routers.passengers  import router as passengers_router
from app.routers.presidents  import router as presidents_router
from app.routers.sources     import router as sources_router
from app.routers.meta        import router as meta_router

from app.config import get_settings

s = get_settings()
app = FastAPI(title=s.APP_TITLE)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[str(o) for o in s.CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(voyages_router)
app.include_router(passengers_router)
app.include_router(presidents_router)
app.include_router(sources_router)
app.include_router(meta_router)
app.include_router(submissions.router, prefix="/api/submissions", tags=["submissions"])


@app.get("/", tags=["root"])
def read_root():
    return {"message": "Welcome to the Sequoia API"}
