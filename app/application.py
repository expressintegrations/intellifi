from ExpressIntegrations.Utils import Utils
from fastapi import FastAPI
from google.cloud import firestore

from . import endpoints, functions
from .containers import Container


def create_app(env: str = 'prod') -> FastAPI:
    container = Container()
    # Load the config variables
    container.config.from_yaml(f'etc/config-{env}.yaml')

    db = firestore.Client()

    # Set the Emerge properties
    auth_doc = (
        db
        .collection(container.config.get('emerge.firestore.collection'))
        .document(container.config.get('emerge.firestore.auth_document'))
    )
    auth = auth_doc.get().to_dict()
    container.config.emerge.access_token.from_value(
        auth[container.config.get('emerge.firestore.access_token.location')]
    )

    # Set the HubSpot properties
    auth_doc = (
        db
        .collection(container.config.get('hubspot.firestore.collection'))
        .document(container.config.get('hubspot.firestore.auth_document'))
    )
    auth = auth_doc.get().to_dict()
    container.config.hubspot.access_token.from_value(auth['access_token'])
    container.config.hubspot.expires_at.from_value(auth['expires_at'])
    container.config.hubspot.refresh_token.from_value(auth['refresh_token'])

    # Get the HubSpot client secret
    container.config.hubspot.client_secret.from_value(
        Utils.access_secret_version(
            container.config.get('gcloud.project'),
            container.config.get('hubspot.client_secret.location'),
            container.config.get('hubspot.client_secret.version')
        )
    )

    # Get the Pandadoc api key
    container.config.pandadoc.api_key.from_value(
        Utils.access_secret_version(
            container.config.get('gcloud.project'),
            container.config.get('pandadoc.api_key_secret.location'),
            container.config.get('pandadoc.api_key_secret.version')
        )
    )

    # Wire up the endpoints for dependency injection
    container.wire(modules=[endpoints, functions])

    # Initialize the API with the endpoints
    app = FastAPI()
    app.container = container
    app.include_router(endpoints.router)
    return app
