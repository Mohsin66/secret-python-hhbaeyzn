import uvicorn
from src.globals.fastapi_server import app  # Import FastAPI app from your server file

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
