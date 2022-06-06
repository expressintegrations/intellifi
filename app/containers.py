from dependency_injector import containers, providers
from google.cloud import firestore
from google.cloud import tasks_v2

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
