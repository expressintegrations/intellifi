from fastapi import FastAPI

from . import endpoints
from . import functions
from .containers import Container


def create_app(env='prod') -> FastAPI:
  container = Container()
  # Load the config variables
  container.config.from_yaml(f'etc/config-{env}.yaml')

  # Get the stored value of the access token and when it expires
  # db = firestore.Client()
  # auth_doc = (
  #     db
  #     .collection(container.config.get('acuity.firestore.collection'))
  #     .document(container.config.get('acuity.firestore.document'))
  # )
  # auth = auth_doc.get().to_dict()
  # container.config.trinet.access_token.from_value(auth['access_token'])
  # container.config.trinet.expires_at.from_value(auth['expires_at'])

  # Get the acuity client secret
  # container.config.trinet.client_secret.from_value(
  #     Utils.access_secret_version(
  #         container.config.get('gcloud.project'),
  #         container.config.get('acuity.client_secret.location'),
  #         container.config.get('acuity.client_secret.version')
  #     )
  # )

  # Wire up the endpoints for dependency injection
  container.wire(modules=[endpoints, functions])

  # Initialize the API with the endpoints
  app = FastAPI()
  app.container = container
  app.include_router(endpoints.router)
  return app
