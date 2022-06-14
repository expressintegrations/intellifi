from fastapi import FastAPI

from app.application import create_app

app = create_app()

if __name__ == "__main__":
    # Use this for development purposes only
    app: FastAPI = create_app('dev')
    import uvicorn

    # noinspection PyTypeChecker
    uvicorn.run(app, host = "127.0.0.1", port = 8080, log_level = "debug")
