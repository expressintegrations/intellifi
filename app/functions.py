from dependency_injector.wiring import inject, Provide
from fastapi import Depends
from google.cloud import logging

from .containers import Container
from .models import HubSpotCompanySyncRequest, HubSpotDealSyncRequest
from .services import EmergeService, HubSpotService

log_name = 'intellifi.functions'
logging_client = logging.Client()
logger = logging_client.logger(log_name)


@inject
def sync_emerge_company_to_hubspot(
    hubspot_company_sync_request: HubSpotCompanySyncRequest,
    emerge_service: EmergeService = Depends(Provide[Container.emerge_service]),
    hubspot_service: HubSpotService = Depends(Provide[Container.hubspot_service])
):
    if not hubspot_company_sync_request.emerge_company_id:
        logger.log_text(
            f"Emerge Company ID was blank for Company {hubspot_company_sync_request.object_id}. Skip processing...",
            severity = 'DEBUG'
        )
        return
    emerge_company = emerge_service.get_customer_billing_info(
        company_id = hubspot_company_sync_request.emerge_company_id,
        year = hubspot_company_sync_request.year,
        month = hubspot_company_sync_request.month
    )
    update_result = hubspot_service.update_company(
        company_id = hubspot_company_sync_request.object_id,
        properties = emerge_company.to_hubspot_company()
    )
    logger.log_text(
        f"Company update result for {hubspot_company_sync_request.object_id}: {update_result}",
        severity = 'DEBUG'
    )


@inject
def associate_customer_deal(
    hubspot_deal_sync_request: HubSpotDealSyncRequest,
    hubspot_service: HubSpotService = Depends(Provide[Container.hubspot_service])
):
    associations = hubspot_service.get_company_for_deal(
        deal_id = hubspot_deal_sync_request.object_id
    )
    company_id = None
    if len(associations.results) == 0:
        deal = hubspot_service.get_deal(
            deal_id = hubspot_deal_sync_request.object_id,
            property_names = ['original_closed_won_deal', 'dealname']
        )
        deal_name = deal['properties']['dealname'].replace('Customer Deal - ', '')
        company_id = get_or_create_hubspot_company_by_name(
            company_name = deal_name
        )

        original_deal_id = deal['properties'].get('original_closed_won_deal')
        if not original_deal_id or len(original_deal_id) == 0:
            deals = hubspot_service.get_deal_by_name(
                deal_name = deal_name
            )
            if deals['total'] == 0:
                # This should never happen
                raise Exception(
                    f"No deals found with name {deal_name}: {deals}"
                )
            elif deals['total'] == 1:
                original_deal = deals['results'][0]
                logger.log_text(
                    f"Found Deal in HubSpot with name {deal_name}: {original_deal['id']}",
                    severity = 'DEBUG'
                )
                original_deal_id = original_deal['id']
                # update the original closed won deal property
                update_result = hubspot_service.update_deal(
                    deal_id = hubspot_deal_sync_request.object_id,
                    properties = {
                        "original_closed_won_deal": original_deal_id
                    }
                )
                logger.log_text(
                    f"Update customer deal with original deal ID result: {update_result}",
                    severity = 'DEBUG'
                )
            else:
                # This should never happen
                raise Exception(
                    f"Multiple deals found with name {deal_name}: {deals}"
                )
        # associate the company to the original deal
        company_association_result = hubspot_service.set_company_for_deal(
            deal_id = original_deal_id,
            company_id = company_id
        )
        logger.log_text(
            f"Company association result for original deal {original_deal_id}: {company_association_result}",
            severity = 'DEBUG'
        )
    else:
        company_id = associations.first().id
    company_association_result = hubspot_service.set_customer_company_for_deal(
        deal_id = hubspot_deal_sync_request.object_id,
        company_id = company_id
    )
    logger.log_text(
        f"Company association result for customer deal {hubspot_deal_sync_request.object_id}:"
        f" {company_association_result}",
        severity = 'DEBUG'
    )


@inject
def get_emerge_company(
    hubspot_company_sync_request: HubSpotCompanySyncRequest,
    emerge_service: EmergeService = Depends(Provide[Container.emerge_service])
):
    return emerge_service.get_customer_billing_info(
        company_id = hubspot_company_sync_request.emerge_company_id,
        year = hubspot_company_sync_request.year,
        month = hubspot_company_sync_request.month
    )


@inject
def sync_emerge_companies_to_hubspot(
    emerge_service: EmergeService = Depends(Provide[Container.emerge_service]),
    hubspot_service: HubSpotService = Depends(Provide[Container.hubspot_service])
):
    for customer in emerge_service.get_all_customers():
        companies = hubspot_service.get_company_by_emerge_company(emerge_company_id = customer.company_id)
        logger.log_text(
            f"Companies search result for {customer.company_id}: {companies}",
            severity = 'DEBUG'
        )
        if companies['total'] == 0:
            company_id = get_or_create_hubspot_company_by_name(
                company_name = customer.company_name
            )
            logger.log_text(
                f"No Company found in HubSpot with Emerge Company ID {customer.company_id}. Created company "
                f"{company_id} in HubSpot",
                severity = 'DEBUG'
            )

        elif companies['total'] == 1:
            company = companies['results'][0]
            company_id = company['id']
        else:
            company = companies['results'][0]
            company_id = company['id']
            logger.log_text(
                f"Multiple companies found with Emerge Company ID {customer.company_id}: {companies}",
                severity = 'DEBUG'
            )
            for company_to_merge in companies['results'][1:]:
                hubspot_service.merge_companies(
                    company_to_merge = company_to_merge['id'],
                    company_to_keep = company_id
                )

        # update
        logger.log_text(
            f"Updating company {company_id}",
            severity = 'DEBUG'
        )
        sync_emerge_company_to_hubspot(
            hubspot_company_sync_request = HubSpotCompanySyncRequest(
                object_id = company_id,
                emerge_company_id = customer.company_id
            )
        )


@inject
def get_or_create_hubspot_company_by_name(
    company_name: str,
    hubspot_service: HubSpotService = Depends(Provide[Container.hubspot_service])
):
    companies = hubspot_service.get_company_by_name(
        company_name = company_name
    )
    logger.log_text(
        f"Companies search result for {company_name}: {companies}",
        severity = 'DEBUG'
    )
    if companies['total'] == 0:
        # create a new company
        logger.log_text(
            f"No Company found in HubSpot with name {company_name}. Creating a new one...",
            severity = 'DEBUG'
        )

        properties = {
            "name": company_name,
        }
        company = hubspot_service.create_company(properties = properties)
        return company['id']
    elif companies['total'] == 1:
        company = companies['results'][0]
        logger.log_text(
            f"Found Company in HubSpot with name {company_name}: {company['id']}",
            severity = 'DEBUG'
        )
        return company['id']
    else:
        company = companies['results'][0]
        company_id = company['id']
        logger.log_text(
            f"Multiple companies found with name {company_name}. Merging {companies['total']} companies: {companies}",
            severity = 'DEBUG'
        )
        for company_to_merge in companies['results'][1:]:
            hubspot_service.merge_companies(
                company_to_merge = company_to_merge['id'],
                company_to_keep = company_id
            )
        return company_id