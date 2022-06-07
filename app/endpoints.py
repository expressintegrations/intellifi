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
from fastapi.responses import HTMLResponse

from . import functions

from .containers import Container
from .services import CloudTasksService

from .models import HubSpotCompaniesRequest


router = APIRouter()


@router.get('/intellifi/v1/companies')
@inject
async def get_intellifi_company(
  request: Request
):
  data = await request.form()
  print(**data)
  return {'results': [
    {
      "objectId": 245,
      "title": "API-22: APIs working too fast",
      "link": "http://example.com/1",
      "company": "Test Company",
      "date_opened": 1475447180000,
      "number_of_locations": 2,
      "account_status": "Active",
      "number_of_users": 4,
      "sales_last_month": "Volume: 2, Sales: $500",
      "sales_current_month": "Volume: 20, Sales: $2400.00",
      "sales_ytd": "Volume: 150, Sales: $12,000.00",
      "product_types_last_month": "2 Packages, 4 Individual Reports",
      "product_types_current_month": "4 Packages, 5 Individual Reports",
      "product_types_ytd": "50 Packages, 20 Individual Reports",
      "last_report_run": 1475447180000
    }
  ]}


# @router.post('/acuity/v1/events')
# @inject
# def process_acuity_events(
#     cloud_tasks_service: CloudTasksService = Depends(Provide[Container.cloud_tasks_service]),
#     event: AcuityWebhookEvent = None
# ):
#   try:
#     cloud_tasks_service.enqueue(
#         'acuity/v1/events/worker',
#         payload=event
#     )
#   except Exception:
#     print(traceback.format_exc())
#     raise HTTPException(
#         status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#         detail="Failed to enqueue the acuity event",
#     )
#   return HTMLResponse(status_code=status.HTTP_204_NO_CONTENT)
#
#
# @router.post('/acuity/v1/events/worker')
# def acuity_events_worker(event: AcuityWebhookEvent):
#   try:
#     print(event)
#   except Exception:
#     print(traceback.format_exc())
#     raise HTTPException(
#         status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#         detail="Failed to process the acuity event",
#     )
#   return HTMLResponse(status_code=status.HTTP_204_NO_CONTENT)
