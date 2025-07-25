from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fastapi.staticfiles import StaticFiles




from app.routers.voyages    import router as voyages_router
from app.routers.passengers import router as passengers_router
from app.routers.auth import router as auth_router


# after including other routers:

app = FastAPI(title="Sequoia API")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000",
    "http://ec2-18-191-216-71.us-east-2.compute.amazonaws.com"],  # adjust as needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(auth_router,     prefix="/api/auth",     tags=["auth"])
app.include_router(voyages_router,  prefix="/api/voyages",  tags=["voyages"])
app.include_router(passengers_router, prefix="/api/passengers", tags=["passengers"])


# app.mount("/static", StaticFiles(directory="media"), name="static")



# Root endpoint (optional)
@app.get("/", tags=["root"])
def read_root():
    return {"message": "Welcome to the Sequoia API"}