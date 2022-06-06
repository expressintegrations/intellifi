from app.application import create_app

app = create_app()


if __name__ == "__main__":
  # Use this for development purposes only
  app = create_app('dev')
  import uvicorn
  uvicorn.run(app, host="127.0.0.1", port=8080, log_level="debug")
