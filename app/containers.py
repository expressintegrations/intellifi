from dependency_injector import containers, providers
from google.cloud import firestore
from google.cloud import tasks_v2
from ExpressIntegrations.HubSpot import hubspot
from ExpressIntegrations.Emerge import emerge

from . import services


class Container(containers.DeclarativeContainer):

  config = providers.Configuration()

  firestore_client = providers.Factory(
      firestore.Client
  )

  cloud_tasks_client = providers.Factory(
      tasks_v2.CloudTasksClient
  )

  cloud_tasks_service = providers.Factory(
      services.CloudTasksService,
      cloud_tasks_client=cloud_tasks_client,
      project=config.gcloud.project,
      location=config.gcloud.location,
      queue=config.gcloud.tasks.queue,
      base_url=config.gcloud.base_url,
      service_account_email=config.gcloud.tasks.service_account_email
  )

  hubspot_client = providers.Factory(
    hubspot.hubspot,
    access_token=config.hubspot.access_token,
    expires_at=config.hubspot.expires_at,
    client_id=config.hubspot.client_id,
    client_secret=config.hubspot.client_secret,
    refresh_token=config.hubspot.refresh_token
  )

  hubspot_service = providers.Factory(
    services.HubSpotService,
    firestore_collection=config.hubspot.firestore.collection,
    auth_document=config.hubspot.firestore.auth_document,
    access_token_location=config.hubspot.firestore.access_token.location,
    expires_at_location=config.hubspot.firestore.expires_at.location,
    hubspot_client=hubspot_client,
    firestore_client=firestore_client
  )

  emerge_client = providers.Factory(
    emerge.emerge,
    environment=config.emerge.environment,
    access_token=config.emerge.access_token
  )

  emerge_service = providers.Factory(
    services.EmergeService,
    emerge_client=emerge_client
  )
