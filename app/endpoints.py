from fastapi import APIRouter

# from .models import AcuityWebhookEvent


router = APIRouter()


@router.get('/intellifi/v1')
def home():
  return {'hello': 'world'}


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
