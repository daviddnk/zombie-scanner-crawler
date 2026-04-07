print(">>> DASHBOARD VERSION 9.0 LOADED <<<")

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import asyncpg
import os

app = FastAPI()

# Carpeta donde están las plantillas HTML
templates = Jinja2Templates(directory="templates")

# URL de la base de datos desde Railway
DATABASE_URL = os.getenv("DATABASE_URL")


async def get_tokens():
    """
    Obtiene todos los registros de la tabla abandoned_tokens.
    Se eliminó ORDER BY timestamp porque esa columna NO existe.
    """
    conn = await asyncpg.connect(DATABASE_URL)
    rows = await conn.fetch("SELECT * FROM abandoned_tokens")
    await conn.close()
    return rows


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    tokens = await get_tokens()
    return templates.TemplateResponse("index.html", {"request": request, "tokens": tokens})
