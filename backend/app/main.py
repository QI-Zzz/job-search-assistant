from fastapi import FastAPI
from app.routers.jobs import router as jobs_router

app = FastAPI()
app.include_router(jobs_router)

@app.get("/health")
async def health_check():
    return {"status": "ok"}