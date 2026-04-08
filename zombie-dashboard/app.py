print(">>> DASHBOARD API MODE 1.0 <<<")

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import asyncpg
import os

app = FastAPI()

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

DATABASE_URL = os.getenv("DATABASE_URL")

async def get_tokens():
    conn = await asyncpg.connect(DATABASE_URL)
    rows = await conn.fetch("SELECT * FROM abandoned_tokens")
    await conn.close()
    return [dict(r) for r in rows]

@app.get("/tokens")
async def tokens():
    return await get_tokens()

@app.get("/")
async def root():
    return FileResponse("templates/index.html")
