import json

from ExpressIntegrations.Emerge import emerge
from ExpressIntegrations.HubSpot import hubspot
from google.cloud import firestore, logging
from google.cloud import tasks_v2

from .models import EmergeCompanyBillingInfo, EmergeCompanyInfo

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
    self.logger.log_text(f"Enqueueing task on {self.base_url}/{relative_handler_uri}", severity='DEBUG')
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
      converted_payload = json.dumps(payload).encode()

      # Add the payload to the request.
      task['http_request']['body'] = converted_payload

    response = self.cloud_tasks_client.create_task(request={'parent': parent, 'task': task})

    self.logger.log_text(f"Created task {response.name} on {self.base_url}{relative_handler_uri}", severity='DEBUG')


class EmergeService(BaseService):

  def __init__(
    self,
    emerge_client: emerge.emerge
  ) -> None:
    self.emerge_client = emerge_client
    super().__init__()

  def get_all_customers(self):
    self.logger.log_text('Getting all customers', severity='DEBUG')
    return [EmergeCompanyInfo.parse_obj(customer) for customer in self.emerge_client.customers(0, 1000000000)]

  def get_customer_billing_info(self, company_id: int, year: int, month: int):
    self.logger.log_text(f"Getting customer {company_id}", severity='DEBUG')
    billing_info = self.emerge_client.customer_billing_info(
      company_id=company_id,
      year=year,
      month=month
    ) if company_id else {}
    return EmergeCompanyBillingInfo.parse_obj(billing_info)


class HubSpotService(BaseService):

  def __init__(
    self,
    firestore_collection: str,
    auth_document: str,
    access_token_location: str,
    expires_at_location: str,
    hubspot_client: hubspot.hubspot,
    firestore_client: firestore.Client
  ) -> None:
    self.firestore_collection = firestore_collection
    self.auth_document = auth_document
    self.access_token_location = access_token_location
    self.expires_at_location = expires_at_location
    self.hubspot_client = hubspot_client
    self.firestore_client = firestore_client
    auth_doc = self.firestore_client.collection(self.firestore_collection).document(self.auth_document)
    auth = auth_doc.get().to_dict()
    if self.hubspot_client.auth_refreshed:
      auth[self.access_token_location] = self.hubspot_client.access_token
      auth[self.expires_at_location] = self.hubspot_client.expires_at
      auth_doc.set(auth, merge=True)
      self.hubspot_client.auth_refreshed = False
    super().__init__()

  def update_company(self, company_id, properties):
    self.logger.log_text('Updating company', severity='DEBUG')
    return self.hubspot_client.update_record(
      object_type='companies',
      object_id=company_id,
      properties=properties
    )['content']

  def get_company_by_emerge_company(self, emerge_company_id=None, property_names=[], after=None, sorts=[]):
    self.logger.log_text('Getting company by emerge company', severity='DEBUG')
    return self.hubspot_client.search_records_by_property_value(
      object_type='companies',
      property_name='emerge_company_id',
      property_value=emerge_company_id,
      property_names=property_names,
      after=after,
      sorts=sorts
    )['content']