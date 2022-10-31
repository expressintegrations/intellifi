import base64
import hashlib
import hmac
import traceback
from typing import List

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse
from google.cloud import logging

from . import functions
from .containers import Container
from .models import (
    HubSpotCompanySyncRequest,
    HubSpotDealSyncRequest,
    HubSpotWebhookEvent,
    HubSpotLineItemSyncRequest,
    PandadocProposalRequest
)
from .services import CloudTasksService, FirestoreService

log_name = 'intellifi.endpoints'
logging_client = logging.Client()
logger = logging_client.logger(log_name)

router = APIRouter()


@router.post('/intelifi/v1/proposal')
async def get_proposal_session(
    request: Request
):
    body = await request.json()
    print(body)
    pandadoc_proposal_request = PandadocProposalRequest.parse_obj(body)
    return functions.get_pandadoc_proposal_session(pandadoc_proposal_request=pandadoc_proposal_request)


@router.get('/intellifi/v1/companies')
@inject
async def get_emerge_company_crm_card(
    request: Request,
    cloud_tasks_service: CloudTasksService = Depends(Provide[Container.cloud_tasks_service]),
    webhook_secret_key: str = Depends(Provide[Container.config.hubspot.client_secret]),
    user_id: int = Query(default=None, alias='userId'),
    user_email: str = Query(default=None, alias='userEmail'),
    associated_object_id: int = Query(default=None, alias='associatedObjectId'),
    associated_object_type: str = Query(default=None, alias='associatedObjectType'),
    portal_id: int = Query(default=None, alias='portalId'),
    emerge_company_id: str = None
):
    if 'x-hubspot-signature-v3' not in request.headers or 'x-hubspot-request-timestamp' not in request.headers:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not authorized",
        )
    expected_sig = request.headers.get('x-hubspot-signature-v3')
    body = await request.body()
    message = f"{request.method}{str(request.url).replace('http://', 'https://')}{body.decode()}" \
              f"{request.headers.get('x-hubspot-request-timestamp')}"
    computed_sha = hmac.new(key=webhook_secret_key.encode(), msg=message.encode(), digestmod=hashlib.sha256)
    my_sig = base64.b64encode(computed_sha.digest()).decode()
    if my_sig != expected_sig:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not authorized",
        )
    emerge_company_id = int(emerge_company_id) if emerge_company_id and emerge_company_id != '' else None
    logger.log_text(
        f"User {user_email}({user_id}) requested Emerge CRM card for Company: "
        f"{emerge_company_id} on {associated_object_type} ({associated_object_id}) for portal {portal_id}",
        severity='DEBUG'
    )

    hubspot_company_sync_request = HubSpotCompanySyncRequest(
        object_id=associated_object_id,
        emerge_company_id=emerge_company_id
    )

    if associated_object_type == 'COMPANY':
        cloud_tasks_service.enqueue(
            'hubspot/v1/company-sync/worker',
            payload=hubspot_company_sync_request.dict()
        )
    return functions.get_emerge_company(
        hubspot_company_sync_request=hubspot_company_sync_request
    ).to_hubspot_crm_card()


@router.post('/hubspot/v1/events')
@inject
async def process_hubspot_events(
    request: Request,
    cloud_tasks_service: CloudTasksService = Depends(Provide[Container.cloud_tasks_service]),
    webhook_secret_key: str = Depends(Provide[Container.config.hubspot.client_secret]),
    firestore_service: FirestoreService = Depends(Provide[Container.firestore_service]),
    events: List[HubSpotWebhookEvent] = tuple()
):
    expected_sig = request.headers['x-hubspot-signature-v3']
    body = await request.body()
    message = f"{request.method}{str(request.url).replace('http://', 'https://')}{body.decode()}" \
              f"{request.headers['x-hubspot-request-timestamp']}"
    computed_sha = hmac.new(key=webhook_secret_key.encode(), msg=message.encode(), digestmod=hashlib.sha256)
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
                    'hubspot/v1/company-sync/worker',
                    payload=HubSpotCompanySyncRequest(
                        object_id=event.objectId,
                        emerge_company_id=int(event.propertyValue) if event.propertyValue and len(
                            event.propertyValue
                        ) > 0 else None
                    ).dict()
                )

            if event.propertyName == 'customer_deal' and event.subscriptionType == 'deal.propertyChange':
                cloud_tasks_service.enqueue(
                    'hubspot/v1/deal-sync/worker',
                    payload=HubSpotDealSyncRequest(
                        object_id=event.objectId
                    ).dict()
                )

            if event.propertyName == 'pricing_tier' and event.subscriptionType == 'deal.propertyChange':
                if not firestore_service.line_item_sync_enabled():
                    print(f"Line Item Sync is disabled. Skip webhook: {event}")
                    return
                cloud_tasks_service.enqueue(
                    'hubspot/v1/line-item-sync/worker',
                    payload=HubSpotLineItemSyncRequest(
                        object_id=event.objectId,
                        pricing_tier=event.propertyValue if event.propertyValue != '' else None
                    ).dict()
                )
    except Exception:
        logger.log_text(
            traceback.format_exc(),
            severity='DEBUG'
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to enqueue the hubspot event",
        )
    return HTMLResponse(status_code=status.HTTP_204_NO_CONTENT)


@router.post('/hubspot/v1/line-item-sync/worker')
def hubspot_line_item_sync_worker(event: HubSpotLineItemSyncRequest):
    try:
        functions.sync_line_items(sync_request=event)
    except Exception:
        print(traceback.format_exc())
        logger.log_text(
            traceback.format_exc(),
            severity='DEBUG'
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process the hubspot deal pricing tier event",
        )
    return HTMLResponse(status_code=status.HTTP_204_NO_CONTENT)


@router.post('/hubspot/v1/company-sync/worker')
def hubspot_company_sync_worker(event: HubSpotCompanySyncRequest):
    try:
        functions.sync_emerge_company_to_hubspot(hubspot_company_sync_request=event)
    except Exception:
        logger.log_text(
            traceback.format_exc(),
            severity='DEBUG'
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process the hubspot company event",
        )
    return HTMLResponse(status_code=status.HTTP_204_NO_CONTENT)


@router.post('/hubspot/v1/deal-sync/worker')
def hubspot_deal_sync_worker(event: HubSpotDealSyncRequest):
    try:
        functions.associate_customer_deal(hubspot_deal_sync_request=event)
    except Exception:
        logger.log_text(
            traceback.format_exc(),
            severity='DEBUG'
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process the hubspot deal event",
        )
    return HTMLResponse(status_code=status.HTTP_204_NO_CONTENT)


@router.post('/intellifi/v1/companies/sync')
def sync_emerge_companies_to_hubspot(
    request: Request,
    force: bool = False
):
    if request.headers.get('x-cloudscheduler-jobname') != 'intellifi_companies_sync':
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not authorized",
        )
    try:
        functions.sync_emerge_companies_to_hubspot(force=force)
    except Exception:
        logger.log_text(
            traceback.format_exc(),
            severity='DEBUG'
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to sync the emerge companies",
        )
    return HTMLResponse(status_code=status.HTTP_204_NO_CONTENT)
