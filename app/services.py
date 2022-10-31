import json
from time import sleep

import pandadoc_client
from ExpressIntegrations.Emerge import emerge
from ExpressIntegrations.HubSpot import hubspot
from google.cloud import firestore, logging, tasks_v2
from pandadoc_client.api import documents_api
from pandadoc_client.model.document_create_by_template_request_tokens import DocumentCreateByTemplateRequestTokens
from pandadoc_client.model.document_create_link_request import DocumentCreateLinkRequest
from pandadoc_client.model.document_create_request import DocumentCreateRequest
from pandadoc_client.model.document_create_request_recipients import DocumentCreateRequestRecipients
from pandadoc_client.model.document_send_request import DocumentSendRequest
from pandadoc_client.model.pricing_table_request import PricingTableRequest
from pandadoc_client.model.pricing_table_request_row_options import PricingTableRequestRowOptions
from pandadoc_client.model.pricing_table_request_rows import PricingTableRequestRows
from pandadoc_client.model.pricing_table_request_sections import PricingTableRequestSections

from .models import EmergeCompanyBillingInfo, EmergeCompanyInfo, HubSpotAssociationBatchReadResponse, \
    PandadocProposalRequest

log_name = 'intellifi.services'


class BaseService:

    def __init__(self) -> None:
        logging_client = logging.Client()
        self.logger = logging_client.logger(log_name)


class PandadocService:
    TEMPLATE_UUID = 'kYQHXrqWKwcbav3igdjdDf'
    FOLDER_UUID = 'xU8wet8Qy99dojkkUAMi9d'
    PRICING_TABLE_NAME = 'Additional Products'
    BACKGROUND_CHECK_SECTION_TITLE = 'Background Checks'
    DRUG_TESTS_SECTION_TITLE = 'Drug Tests'
    RECIPIENT_ROLE = 'Client'
    TOKEN_COMPANY_NAME = 'company_name'
    TOKEN_PACKAGE_1_PRICE = 'package_1_price'
    TOKEN_PACKAGE_2_PRICE = 'package_2_price'
    TOKEN_PACKAGE_3_PRICE = 'package_3_price'
    TOKEN_PREPARED_BY = 'prepared_by'
    MAX_CHECK_RETRIES = 5
    DOCUMENT_LIFETIME = 1814400

    def __init__(
        self,
        pandadoc_api_client: pandadoc_client.ApiClient,
    ) -> None:
        self.pandadoc_api_client = pandadoc_api_client
        self.api_instance = documents_api.DocumentsApi(self.pandadoc_api_client)
        super().__init__()

    def get_proposal_session(
        self,
        pandadoc_proposal_request: PandadocProposalRequest
    ):
        pricing_tables = [
            PricingTableRequest(
                name=self.PRICING_TABLE_NAME,
                data_merge=True,
                sections=[
                    PricingTableRequestSections(
                        title=self.BACKGROUND_CHECK_SECTION_TITLE,
                        default=False,
                        rows=[
                            PricingTableRequestRows(
                                options=PricingTableRequestRowOptions(),
                                data={
                                    "name": item['name'],
                                    "description": item['description'],
                                    "type": "background_check",
                                    "price": item['price'],
                                    "qty": 1
                                }
                            ) for item in pandadoc_proposal_request.background_check_line_items
                        ]
                    ),
                    PricingTableRequestSections(
                        title=self.DRUG_TESTS_SECTION_TITLE,
                        default=False,
                        rows=[
                            PricingTableRequestRows(
                                options=PricingTableRequestRowOptions(),
                                data={
                                    "name": item['name'],
                                    "description": item['description'],  # fix this
                                    "type": "drug_test",
                                    "price": item['price'],
                                    "qty": 1
                                }
                            ) for item in pandadoc_proposal_request.drug_test_line_items
                        ]
                    )
                ]
            )
        ]
        document_create_request = DocumentCreateRequest(
            name=f"Proposal - {pandadoc_proposal_request.deal_name}",
            template_uuid=self.TEMPLATE_UUID,
            folder_id=self.FOLDER_UUID,
            recipients=[
                DocumentCreateRequestRecipients(
                    email=pandadoc_proposal_request.email,
                    first_name=pandadoc_proposal_request.first_name,
                    last_name=pandadoc_proposal_request.last_name,
                    role=self.RECIPIENT_ROLE
                )
            ],
            tokens=[
                DocumentCreateByTemplateRequestTokens(
                    name=self.TOKEN_COMPANY_NAME,
                    value=pandadoc_proposal_request.company_name
                ),
                DocumentCreateByTemplateRequestTokens(
                    name=self.TOKEN_PACKAGE_1_PRICE,
                    value=pandadoc_proposal_request.package_1_price
                ),
                DocumentCreateByTemplateRequestTokens(
                    name=self.TOKEN_PACKAGE_2_PRICE,
                    value=pandadoc_proposal_request.package_2_price
                ),
                DocumentCreateByTemplateRequestTokens(
                    name=self.TOKEN_PACKAGE_3_PRICE,
                    value=pandadoc_proposal_request.package_3_price
                ),
                DocumentCreateByTemplateRequestTokens(
                    name=self.TOKEN_PREPARED_BY,
                    value=pandadoc_proposal_request.prepared_by
                ),
            ],
            pricing_tables=pricing_tables
        )
        document = self.api_instance.create_document(document_create_request=document_create_request)
        self.ensure_document_created(document=document)
        self.send_document(document=document)
        return self.get_document_session(recipient=pandadoc_proposal_request.email, document=document)

    def ensure_document_created(self, document):
        retries = 0
        while retries < self.MAX_CHECK_RETRIES:
            sleep(2)
            retries += 1

            doc_status = self.api_instance.status_document(id=document['id'])
            if doc_status.status == 'document.draft':
                return

        raise RuntimeError('Document was not sent')

    def send_document(self, document):
        self.api_instance.send_document(
            id=document['id'],
            document_send_request=DocumentSendRequest(
                silent=True, subject='This doc was send via python SDK'
            ),
        )

    def get_document_session(self, recipient, document):
        return self.api_instance.create_document_link(
            id=document['id'],
            document_create_link_request=DocumentCreateLinkRequest(
                recipient=recipient,
                lifetime=self.DOCUMENT_LIFETIME
            )
        )


class FirestoreService(BaseService):
    def __init__(
        self,
        firestore_client: firestore.Client,
    ) -> None:
        self.firestore_client = firestore_client
        super().__init__()

    def line_item_sync_enabled(self):
        doc = self.firestore_client.collection('hubspot_sync').document('settings')
        settings = doc.get().to_dict()
        return settings['line_item_sync_enabled']


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

        response = self.cloud_tasks_client.create_task(request={'parent': parent, 'task': task})

        self.logger.log_text(
            f"Created task {response.name} on {self.base_url}/{relative_handler_uri}",
            severity='DEBUG'
        )


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
    cache = {}

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
        super().__init__()

    def ensure_auth(self):
        if self.hubspot_client.auth_refreshed:
            auth_doc = self.firestore_client.collection(self.firestore_collection).document(self.auth_document)
            auth = auth_doc.get().to_dict()
            auth[self.access_token_location] = self.hubspot_client.access_token
            auth[self.expires_at_location] = self.hubspot_client.expires_at
            auth_doc.set(auth, merge=True)
            self.hubspot_client.auth_refreshed = False

    def update_company(self, company_id, properties):
        self.ensure_auth()
        self.logger.log_text(f"Updating company {company_id} with properties {properties}", severity='DEBUG')
        return self.hubspot_client.update_record(
            object_type='companies',
            object_id=company_id,
            properties=properties
        )['content']

    def create_company(self, properties):
        self.ensure_auth()
        self.logger.log_text(f"Creating company with properties {properties}", severity='DEBUG')
        return self.hubspot_client.create_record(
            object_type='companies',
            properties=properties
        )['content']

    def get_company_by_emerge_company(
        self,
        emerge_company_id: int = None,
        property_names: list = tuple(),
        after: int = None,
        sorts: list = tuple()
    ):
        self.ensure_auth()
        self.logger.log_text(f"Getting company by emerge company {emerge_company_id}", severity='DEBUG')
        return self.hubspot_client.search_records_by_property_value(
            object_type='companies',
            property_name='emerge_company_id',
            property_value=emerge_company_id,
            property_names=property_names,
            after=after,
            sorts=sorts
        )['content']

    def get_company_by_name(
        self,
        company_name: str = None,
        property_names: list = tuple(),
        after: int = None,
        sorts: list = tuple()
    ):
        self.ensure_auth()
        self.logger.log_text(f"Getting company by name {company_name}", severity='DEBUG')
        return self.hubspot_client.search_records_by_property_value(
            object_type='companies',
            property_name='name',
            property_value=company_name,
            property_names=property_names,
            after=after,
            sorts=sorts
        )['content']

    def get_deal(self, deal_id, property_names=tuple(), associations=None):
        self.ensure_auth()
        self.logger.log_text(f"Getting deal {deal_id}", severity='DEBUG')
        return self.hubspot_client.get_record(
            object_type='deals',
            object_id=deal_id,
            property_names=property_names,
            associations=associations
        )['content']

    def update_deal(self, deal_id, properties):
        self.ensure_auth()
        self.logger.log_text(f"Updating deal {deal_id} with properties {properties}", severity='DEBUG')
        return self.hubspot_client.update_record(
            object_type='deals',
            object_id=deal_id,
            properties=properties
        )['content']

    def get_deal_by_name(
        self,
        deal_name: str = None,
        property_names: list = tuple(),
        after: int = None,
        sorts: list = tuple()
    ):
        self.ensure_auth()
        self.logger.log_text(f"Getting deal by name {deal_name}", severity='DEBUG')
        return self.hubspot_client.search_records_by_property_value(
            object_type='deals',
            property_name='dealname',
            property_value=deal_name,
            property_names=property_names,
            after=after,
            sorts=sorts
        )['content']

    def set_customer_company_for_deal(self, deal_id, company_id):
        self.ensure_auth()
        self.logger.log_text(f"Setting customer company {company_id} for deal {deal_id}", severity='DEBUG')
        return self.hubspot_client.associate(
            from_object_type='deals',
            from_object_id=deal_id,
            to_object_type='company',
            to_object_id=company_id,
            association_type='customer_deal'
        )

    def set_company_for_deal(self, deal_id, company_id):
        self.ensure_auth()
        self.logger.log_text(f"Setting company {company_id} for deal {deal_id}", severity='DEBUG')
        return self.hubspot_client.set_company_for_deal(
            deal_id=deal_id,
            company_id=company_id
        )

    def get_company_for_deal(self, deal_id):
        self.ensure_auth()
        self.logger.log_text(f"Getting company for deal {deal_id}", severity='DEBUG')
        resp = self.hubspot_client.get_associations(
            from_object_type='deals',
            to_object_type='companies',
            from_object_id=deal_id
        )['content']
        return HubSpotAssociationBatchReadResponse(
            status=resp['status'],
            results=resp['results'],
            started_at=resp['startedAt'],
            completed_at=resp['completedAt']
        )

    def merge_companies(self, company_to_merge: int, company_to_keep: int):
        self.ensure_auth()
        self.logger.log_text(f"Merging company {company_to_merge} into {company_to_keep}")
        merge_data = {
            "primaryObjectId": company_to_keep,
            "objectIdToMerge": company_to_merge
        }
        return self.hubspot_client.custom_request(
            method='POST',
            endpoint=f"crm/v3/objects/companies/merge",
            data=json.dumps(merge_data)
        )

    def get_line_items_for_deal(self, deal_id):
        self.ensure_auth()
        self.logger.log_text(f"Getting line items for deal {deal_id}", severity='DEBUG')
        resp = self.hubspot_client.get_associations(
            from_object_type='deals',
            to_object_type='line_items',
            from_object_id=deal_id
        )['content']
        return HubSpotAssociationBatchReadResponse(
            status=resp['status'],
            results=resp['results'],
            started_at=resp['startedAt'],
            completed_at=resp['completedAt']
        )

    def get_line_item(self, line_item_id, properties=None):
        self.ensure_auth()
        self.logger.log_text(f"Getting line item {line_item_id} with properties {properties}", severity='DEBUG')
        return self.hubspot_client.get_record(
            object_type='line_item',
            object_id=line_item_id,
            property_names=properties
        )['content']

    def get_line_items(self, line_item_ids, properties=None):
        self.ensure_auth()
        self.logger.log_text(f"Getting line items {line_item_ids} with properties {properties}", severity='DEBUG')
        data = {
            'properties': properties,
            'inputs': [{'id': line_item_id} for line_item_id in line_item_ids]
        }
        return self.hubspot_client.custom_request(
            method='POST',
            endpoint=f"crm/v3/objects/line_items/batch/read",
            data=json.dumps(data)
        )['content']['results']

    def create_line_item(self, properties):
        self.ensure_auth()
        self.logger.log_text(f"Creating line item with properties {properties}", severity='DEBUG')
        return self.hubspot_client.create_record(
            object_type='line_item',
            properties=properties
        )['content']

    def create_line_items(self, line_items):
        self.ensure_auth()
        self.logger.log_text(f"Creating line items {line_items}", severity='DEBUG')
        data = {
            'inputs': [{'properties': line_item} for line_item in line_items]
        }
        return self.hubspot_client.custom_request(
            method='POST',
            endpoint=f"crm/v3/objects/line_items/batch/create",
            data=json.dumps(data)
        )['content']['results']

    def set_deal_for_line_item(self, line_item_id, deal_id):
        self.ensure_auth()
        self.logger.log_text(f"Setting deal {deal_id} for line item {line_item_id}", severity='DEBUG')
        return self.hubspot_client.set_deal_for_line_item(
            deal_id=deal_id,
            line_item_id=line_item_id
        )

    def set_deal_for_line_items(self, line_items, deal_id):
        self.ensure_auth()
        self.logger.log_text(f"Setting deal {deal_id} for line items {line_items}", severity='DEBUG')
        data = {
            'inputs': [
                {
                    'from': {
                        'id': line_item['id']
                    },
                    'to': {
                        'id': deal_id
                    },
                    'types': [
                        {
                            'associationCategory': 'HUBSPOT_DEFINED',
                            'associationTypeId': 20
                        }
                    ]
                } for line_item in line_items
            ]
        }
        return self.hubspot_client.custom_request(
            method='POST',
            endpoint=f"crm/v4/associations/line_items/deals/batch/create",
            data=json.dumps(data)
        )['content']

    def update_line_items(self, records):
        self.ensure_auth()
        self.logger.log_text(
            f"Updating {len(records)} line items",
            severity='DEBUG'
        )
        return self.hubspot_client.update_records_batch(
            object_type='line_item',
            records=records
        )['content']

    def delete_line_item(self, line_item_id):
        self.ensure_auth()
        self.logger.log_text(f"Deleting line item {line_item_id}", severity='DEBUG')
        return self.hubspot_client.delete_record(
            object_type='line_item',
            object_id=line_item_id
        )['content']

    def delete_line_items(self, line_item_ids):
        self.ensure_auth()
        self.logger.log_text(f"Deleting line items {line_item_ids}", severity='DEBUG')
        data = {
            'inputs': [{'id': line_item_id} for line_item_id in line_item_ids]
        }
        return self.hubspot_client.custom_request(
            method='POST',
            endpoint=f"crm/v3/objects/line_items/batch/archive",
            data=json.dumps(data)
        )['content']

    def get_products(self, property_names, after=None):
        self.ensure_auth()
        return self.hubspot_client.get_records(
            object_type='product',
            property_names=property_names,
            after=after
        )['content']

    def get_all_products(self, property_names):
        self.ensure_auth()
        self.logger.log_text(f"Getting products", severity='DEBUG')
        products = []
        result = self.get_products(property_names=property_names)
        products += result['results']
        while result.get('paging'):
            result = self.get_products(property_names=property_names, after=result['paging']['next']['after'])
            products += result['results']
        return {p['id']: p['properties'] for p in products}

    def get_owner_by_email(self, email: str = None):
        self.ensure_auth()
        if not email:
            return None
        self.logger.log_text(f"Getting owner by email {email}", severity='DEBUG')
        if email not in self.cache:
            owner_result = self.hubspot_client.custom_request(
                method='GET',
                endpoint=f"crm/v3/owners?email={email}"
            )['content']
            if len(owner_result['results']) == 0:
                self.cache[email] = None
            else:
                self.cache[email] = owner_result['results'][0]['id']
        return self.cache[email]
