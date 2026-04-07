from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import asyncpg
import os

app = FastAPI()
templates = Jinja2Templates(directory="templates")

DATABASE_URL = os.getenv("DATABASE_URL")

async def get_tokens():
    conn = await asyncpg.connect(DATABASE_URL)
    rows = await conn.fetch("SELECT * FROM abandoned_tokens ORDER BY timestamp DESC")
    await conn.close()
    return rows

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    tokens = await get_tokens()
    return templates.TemplateResponse("index.html", {"request": request, "tokens": tokens})

