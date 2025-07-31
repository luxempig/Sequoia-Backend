# File: app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.voyages    import router as voyages_router
from app.routers.passengers import router as passengers_router
from app.routers.auth       import router as auth_router

app = FastAPI(title="Sequoia API")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://ec2-18-191-216-71.us-east-2.compute.amazonaws.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
# Each router defines its own prefix (e.g. /api/voyages)
app.include_router(auth_router)
app.include_router(voyages_router)
app.include_router(passengers_router)

# Root endpoint (optional)
@app.get("/", tags=["root"])
def read_root():
    return {"message": "Welcome to the Sequoia API"}
