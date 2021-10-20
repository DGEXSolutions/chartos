from fastapi import FastAPI
from .views import router as view_router


app = FastAPI()
app.include_router(view_router)
