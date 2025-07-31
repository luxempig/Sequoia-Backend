# File: app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.voyages     import router as voyages_router
from app.routers.passengers  import router as passengers_router
from app.routers.presidents  import router as presidents_router   # ← NEW

app = FastAPI(title="Sequoia API")

# Enable CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://ec2-18-191-216-71.us-east-2.compute.amazonaws.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(voyages_router)
app.include_router(passengers_router)
app.include_router(presidents_router)       # ← NEW

@app.get("/", tags=["root"])
def read_root():
    return {"message": "Welcome to the Sequoia API"}
