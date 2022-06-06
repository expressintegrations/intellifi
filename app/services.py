from google.cloud import logging
from google.cloud import tasks_v2

log_name = 'intellifi'


class BaseService:

  def __init__(self) -> None:
    logging_client = logging.Client()
    self.logger = logging_client.logger(log_name)


class CloudTasksService(BaseService):

  def __init__(
      self,
      cloud_tasks_client: tasks_v2.CloudTasksClient,
      project: str,
      location: str,
      queue: str,
      base_url: str,
      service_account_email: str
  ) -> None:
    self.cloud_tasks_client = cloud_tasks_client
    self.project = project
    self.location = location
    self.queue = queue
    self.base_url = base_url
    self.service_account_email = service_account_email
    super().__init__()

  def enqueue(
      self,
      relative_handler_uri: str,
      payload: dict = None
  ) -> None:
    self.logger.log_text(f"Enqueueing task on {self.base_url}{relative_handler_uri}", severity='DEBUG')
    parent = self.cloud_tasks_client.queue_path(self.project, self.location, self.queue)

    # Construct the request body.
    task = {
        'http_request': {  # Specify the type of request.
            'http_method': tasks_v2.HttpMethod.POST,
            'url': f"{self.base_url}{relative_handler_uri}",  # The full url path that the task will be sent to.
            'oidc_token': {
                'service_account_email': self.service_account_email,
                'audience': self.base_url
            },
        }
    }

    if payload is not None:
      # The API expects a payload of type bytes.
      converted_payload = payload.encode()

      # Add the payload to the request.
      task['http_request']['body'] = converted_payload

    response = self.cloud_tasks_client.create_task(request={'parent': parent, 'task': task})

    self.logger.log_text(f"Created task {response.name} on {self.base_url}{relative_handler_uri}", severity='DEBUG')
