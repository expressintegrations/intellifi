import base64
import hmac
import traceback

from dependency_injector.wiring import Provide
from dependency_injector.wiring import inject
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from fastapi import Query
from fastapi.responses import HTMLResponse
from google.cloud import logging

from . import functions

from .containers import Container
from .services import CloudTasksService

from .models import HubSpotWebhookEvent, HubSpotCompanySyncRequest
from typing import List

log_name = 'intellifi'
router = APIRouter()
logging_client = logging.Client()
logger = logging_client.logger(log_name)


@router.get('/intellifi/v1/companies')
@inject
async def get_emerge_company_crm_card(
  request: Request,
  cloud_tasks_service: CloudTasksService = Depends(Provide[Container.cloud_tasks_service]),
  webhook_secret_key: str = Depends(Provide[Container.config.hubspot.client_secret]),
  user_id: int = Query(alias='userId'),
  user_email: str = Query(alias='userEmail'),
  associated_object_id: int = Query(alias='associatedObjectId'),
  associated_object_type: str = Query(alias='associatedObjectType'),
  portal_id: int = Query(alias='portalId'),
  emerge_company_id: int = None
):
  expected_sig = request.headers['x-hubspot-signature']
  body = await request.body()
  verify = f"{webhook_secret_key}{request.method}{request.url}{body.decode()}".encode()
  computed_sha = hmac.new(key=webhook_secret_key.encode(), msg=verify, digestmod="sha256")
  my_sig = base64.b64encode(computed_sha.digest()).decode()
  if my_sig != expected_sig:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="You are not authorized",
    )
  logger.log_text(f"User {user_email}({user_id}) requested Emerge CRM card for Company: {emerge_company_id} on {associated_object_type} ({associated_object_id}) for portal {portal_id}", severity='DEBUG')
  hubspot_company_sync_request = HubSpotCompanySyncRequest(
    object_id=associated_object_id,
    emerge_company_id=emerge_company_id
  )
  cloud_tasks_service.enqueue(
    'hubspot/v1/events/worker',
    payload=hubspot_company_sync_request.dict()
  )
  return {'results': [
    functions.get_emerge_company(hubspot_company_sync_request=hubspot_company_sync_request)
  ]}


@router.post('/hubspot/v1/events')
@inject
async def process_hubspot_events(
  request: Request,
  cloud_tasks_service: CloudTasksService = Depends(Provide[Container.cloud_tasks_service]),
  webhook_secret_key: str = Depends(Provide[Container.config.hubspot.client_secret]),
  events: List[HubSpotWebhookEvent] = []
):
  expected_sig = request.headers['x-hubspot-signature']
  body = await request.body()
  verify = f"{webhook_secret_key}{request.method}{request.url}{body.decode()}".encode()
  computed_sha = hmac.new(key=webhook_secret_key.encode(), msg=verify, digestmod="sha256")
  my_sig = base64.b64encode(computed_sha.digest()).decode()
  if my_sig != expected_sig:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="You are not authorized",
    )
  try:
    for event in events:
      if event.propertyName == 'emerge_company_id' and event.subscriptionType == 'company.propertyChange':
        cloud_tasks_service.enqueue(
            'hubspot/v1/events/worker',
            payload=HubSpotCompanySyncRequest(
              object_id=event.objectId,
              emerge_company_id=int(event.propertyValue) if event.propertyValue and len(event.propertyValue) > 0 else None
            ).dict()
        )
  except Exception:
    print(traceback.format_exc())
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to enqueue the hubspot event",
    )
  return HTMLResponse(status_code=status.HTTP_204_NO_CONTENT)


@router.post('/hubspot/v1/events/worker')
def hubspot_events_worker(event: HubSpotCompanySyncRequest):
  try:
    functions.sync_emerge_company_to_hubspot(hubspot_company_sync_request=event)
  except Exception:
    print(traceback.format_exc())
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to process the acuity event",
    )
  return HTMLResponse(status_code=status.HTTP_204_NO_CONTENT)


@router.post('/intellifi/v1/companies/sync')
def sync_emerge_companies_to_hubspot(
  request: Request
):
  if request.headers.get('x-cloudscheduler-jobname') != 'intellifi_companies_sync':
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="You are not authorized",
    )
  try:
    functions.sync_emerge_companies_to_hubspot()
  except Exception:
    print(traceback.format_exc())
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to sync the emerge companies",
    )
  return HTMLResponse(status_code=status.HTTP_204_NO_CONTENT)