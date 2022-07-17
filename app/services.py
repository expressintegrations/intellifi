import json

from ExpressIntegrations.Emerge import emerge
from ExpressIntegrations.HubSpot import hubspot
from google.cloud import firestore, logging, tasks_v2

from .models import EmergeCompanyBillingInfo, EmergeCompanyInfo, HubSpotAssociationBatchReadResponse

log_name = 'intellifi.services'


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
        self.logger.log_text(f"Enqueueing task on {self.base_url}/{relative_handler_uri}", severity = 'DEBUG')
        parent = self.cloud_tasks_client.queue_path(self.project, self.location, self.queue)

        # Construct the request body.
        task = {
            'http_request': {  # Specify the type of request.
                'http_method': tasks_v2.HttpMethod.POST,
                'url': f"{self.base_url}/{relative_handler_uri}",  # The full url path that the task will be sent to.
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

        response = self.cloud_tasks_client.create_task(request = {'parent': parent, 'task': task})

        self.logger.log_text(
            f"Created task {response.name} on {self.base_url}{relative_handler_uri}",
            severity = 'DEBUG'
        )


class EmergeService(BaseService):

    def __init__(
        self,
        emerge_client: emerge.emerge
    ) -> None:
        self.emerge_client = emerge_client
        super().__init__()

    def get_all_customers(self):
        self.logger.log_text('Getting all customers', severity = 'DEBUG')
        return [EmergeCompanyInfo.parse_obj(customer) for customer in self.emerge_client.customers(0, 1000000000)]

    def get_customer_billing_info(self, company_id: int, year: int, month: int):
        self.logger.log_text(f"Getting customer {company_id}", severity = 'DEBUG')
        billing_info = self.emerge_client.customer_billing_info(
            company_id = company_id,
            year = year,
            month = month
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
            auth_doc.set(auth, merge = True)
            self.hubspot_client.auth_refreshed = False
        super().__init__()

    def update_company(self, company_id, properties):
        self.logger.log_text(f"Updating company {company_id} with properties {properties}", severity = 'DEBUG')
        return self.hubspot_client.update_record(
            object_type = 'companies',
            object_id = company_id,
            properties = properties
        )['content']

    def create_company(self, properties):
        self.logger.log_text(f"Creating company with properties {properties}", severity = 'DEBUG')
        return self.hubspot_client.create_record(
            object_type = 'companies',
            properties = properties
        )['content']

    def get_company_by_emerge_company(
        self,
        emerge_company_id: int = None,
        property_names: list = tuple(),
        after: int = None,
        sorts: list = tuple()
    ):
        self.logger.log_text(f"Getting company by emerge company {emerge_company_id}", severity = 'DEBUG')
        return self.hubspot_client.search_records_by_property_value(
            object_type = 'companies',
            property_name = 'emerge_company_id',
            property_value = emerge_company_id,
            property_names = property_names,
            after = after,
            sorts = sorts
        )['content']

    def get_company_by_name(
        self,
        company_name: str = None,
        property_names: list = tuple(),
        after: int = None,
        sorts: list = tuple()
    ):
        self.logger.log_text(f"Getting company by name {company_name}", severity = 'DEBUG')
        return self.hubspot_client.search_records_by_property_value(
            object_type = 'companies',
            property_name = 'name',
            property_value = company_name,
            property_names = property_names,
            after = after,
            sorts = sorts
        )['content']

    def get_deal(self, deal_id, property_names = tuple()):
        self.logger.log_text(f"Getting deal {deal_id}", severity = 'DEBUG')
        return self.hubspot_client.get_record(
            object_type = 'deals',
            object_id = deal_id,
            property_names = property_names
        )['content']

    def update_deal(self, deal_id, properties):
        self.logger.log_text(f"Updating deal {deal_id} with properties {properties}", severity = 'DEBUG')
        return self.hubspot_client.update_record(
            object_type = 'deals',
            object_id = deal_id,
            properties = properties
        )['content']

    def get_deal_by_name(
        self,
        deal_name: str = None,
        property_names: list = tuple(),
        after: int = None,
        sorts: list = tuple()
    ):
        self.logger.log_text(f"Getting deal by name {deal_name}", severity = 'DEBUG')
        return self.hubspot_client.search_records_by_property_value(
            object_type = 'deals',
            property_name = 'dealname',
            property_value = deal_name,
            property_names = property_names,
            after = after,
            sorts = sorts
        )['content']

    def set_customer_company_for_deal(self, deal_id, company_id):
        self.logger.log_text(f"Setting customer company {company_id} for deal {deal_id}", severity = 'DEBUG')
        return self.hubspot_client.associate(
            from_object_type = 'deals',
            from_object_id = deal_id,
            to_object_type = 'company',
            to_object_id = company_id,
            association_type = 'customer_deal'
        )

    def set_company_for_deal(self, deal_id, company_id):
        self.logger.log_text(f"Setting company {company_id} for deal {deal_id}", severity = 'DEBUG')
        return self.hubspot_client.set_company_for_deal(
            deal_id = deal_id,
            company_id = company_id
        )

    def get_company_for_deal(self, deal_id):
        self.logger.log_text(f"Getting company for deal {deal_id}", severity = 'DEBUG')
        resp = self.hubspot_client.get_associations(
            from_object_type = 'deals',
            to_object_type = 'companies',
            from_object_id = deal_id
        )['content']
        return HubSpotAssociationBatchReadResponse(
            status = resp['status'],
            results = resp['results'],
            started_at = resp['startedAt'],
            completed_at = resp['completedAt']
        )

    def merge_companies(self, company_to_merge: int, company_to_keep: int):
        self.logger.log_text(f"Merging company {company_to_merge} into {company_to_keep}")
        merge_data = {
            "primaryObjectId": company_to_keep,
            "objectIdToMerge": company_to_merge
        }
        return self.hubspot_client.custom_request(
            method = 'POST',
            endpoint = f"crm/v3/objects/companies/merge",
            data = json.dumps(merge_data)
        )
