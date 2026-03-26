"""Vercel entry point for optionstrike backend.

This file serves as the entry point for Vercel serverless deployment.
It exposes the FastAPI app instance that Vercel will serve.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

from db import Base, engine
from api import gex, greeks, symbols, positional

# Create tables on first request (Vercel serverless)
Base.metadata.create_all(bind=engine)

# Create FastAPI app
app = FastAPI(title="optionstrike API")

# Get frontend URL from environment or allow all for now
# You should set FRONTEND_URL in Vercel dashboard after deploying frontend
FRONTEND_URL = os.getenv("FRONTEND_URL", "")
DEFAULT_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

if FRONTEND_URL:
    DEFAULT_ORIGINS.append(FRONTEND_URL)

app.add_middleware(
    CORSMiddleware,
    allow_origins=DEFAULT_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include all routers
app.include_router(gex.router)
app.include_router(greeks.router)
app.include_router(symbols.router)
app.include_router(positional.router)


@app.get("/")
def read_root():
    return {"status": "Backend is running", "mode": "read-only (historical data)"}
