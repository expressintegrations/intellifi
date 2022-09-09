from dependency_injector.wiring import inject, Provide
from fastapi import Depends
from google.cloud import logging

from .containers import Container
from .models import HubSpotCompanySyncRequest, HubSpotDealSyncRequest, HubSpotLineItemSyncRequest, PricingTier
from .services import CloudTasksService, EmergeService, HubSpotService

log_name = 'intellifi.functions'
logging_client = logging.Client()
logger = logging_client.logger(log_name)

PRODUCT_PROPERTIES = ['name', 'price', 'tier_2', 'tier_3', 'hs_product_id']


@inject
def sync_emerge_company_to_hubspot(
    hubspot_company_sync_request: HubSpotCompanySyncRequest,
    emerge_service: EmergeService = Depends(Provide[Container.emerge_service]),
    hubspot_service: HubSpotService = Depends(Provide[Container.hubspot_service])
):
    if not hubspot_company_sync_request.emerge_company_id:
        logger.log_text(
            f"Emerge Company ID was blank for Company {hubspot_company_sync_request.object_id}. Skip processing...",
            severity='DEBUG'
        )
        return
    emerge_company = emerge_service.get_customer_billing_info(
        company_id=hubspot_company_sync_request.emerge_company_id,
        year=hubspot_company_sync_request.year,
        month=hubspot_company_sync_request.month
    )
    hubspot_company_id = hubspot_company_sync_request.object_id
    if not hubspot_company_id:
        companies = hubspot_service.get_company_by_emerge_company(
            emerge_company_id=hubspot_company_sync_request.emerge_company_id
        )
        logger.log_text(
            f"Companies search result for {hubspot_company_sync_request.emerge_company_id}: {companies}",
            severity='DEBUG'
        )
        if companies['total'] == 0:
            hubspot_company_id = get_or_create_hubspot_company_by_name(
                company_name=emerge_company.company_name
            )
            logger.log_text(
                f"No Company found in HubSpot with Emerge Company ID {hubspot_company_sync_request.emerge_company_id}. "
                f"Created company {hubspot_company_id} in HubSpot",
                severity='DEBUG'
            )

        elif companies['total'] == 1:
            company = companies['results'][0]
            hubspot_company_id = company['id']
        else:
            company = companies['results'][0]
            hubspot_company_id = company['id']
            logger.log_text(
                f"Multiple companies found with Emerge Company ID {hubspot_company_sync_request.emerge_company_id}: "
                f"{companies}",
                severity='DEBUG'
            )
            for company_to_merge in companies['results'][1:]:
                hubspot_service.merge_companies(
                    company_to_merge=company_to_merge['id'],
                    company_to_keep=hubspot_company_id
                )
    update_result = hubspot_service.update_company(
        company_id=hubspot_company_id,
        properties=emerge_company.to_hubspot_company()
    )
    logger.log_text(
        f"Company update result for {hubspot_company_id}: {update_result}",
        severity='DEBUG'
    )


@inject
def associate_customer_deal(
    hubspot_deal_sync_request: HubSpotDealSyncRequest,
    hubspot_service: HubSpotService = Depends(Provide[Container.hubspot_service])
):
    associations = hubspot_service.get_company_for_deal(
        deal_id=hubspot_deal_sync_request.object_id
    )
    if len(associations.results) == 0:
        deal = hubspot_service.get_deal(
            deal_id=hubspot_deal_sync_request.object_id,
            property_names=['original_closed_won_deal', 'dealname']
        )
        deal_name = deal['properties']['dealname'].replace('Customer Deal - ', '')
        company_id = get_or_create_hubspot_company_by_name(
            company_name=deal_name
        )

        original_deal_id = deal['properties'].get('original_closed_won_deal')
        if not original_deal_id or len(original_deal_id) == 0:
            deals = hubspot_service.get_deal_by_name(
                deal_name=deal_name
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
                    severity='DEBUG'
                )
                original_deal_id = original_deal['id']
                # update the original closed won deal property
                update_result = hubspot_service.update_deal(
                    deal_id=hubspot_deal_sync_request.object_id,
                    properties={
                        "original_closed_won_deal": original_deal_id
                    }
                )
                logger.log_text(
                    f"Update customer deal with original deal ID result: {update_result}",
                    severity='DEBUG'
                )
            else:
                # This should never happen
                raise Exception(
                    f"Multiple deals found with name {deal_name}: {deals}"
                )
        # associate the company to the original deal
        company_association_result = hubspot_service.set_company_for_deal(
            deal_id=original_deal_id,
            company_id=company_id
        )
        logger.log_text(
            f"Company association result for original deal {original_deal_id}: {company_association_result}",
            severity='DEBUG'
        )
    else:
        company_id = associations.first().id
    company_association_result = hubspot_service.set_customer_company_for_deal(
        deal_id=hubspot_deal_sync_request.object_id,
        company_id=company_id
    )
    logger.log_text(
        f"Company association result for customer deal {hubspot_deal_sync_request.object_id}:"
        f" {company_association_result}",
        severity='DEBUG'
    )


@inject
def get_emerge_company(
    hubspot_company_sync_request: HubSpotCompanySyncRequest,
    emerge_service: EmergeService = Depends(Provide[Container.emerge_service])
):
    return emerge_service.get_customer_billing_info(
        company_id=hubspot_company_sync_request.emerge_company_id,
        year=hubspot_company_sync_request.year,
        month=hubspot_company_sync_request.month
    )


@inject
def sync_emerge_companies_to_hubspot(
    emerge_service: EmergeService = Depends(Provide[Container.emerge_service]),
    cloud_tasks_service: CloudTasksService = Depends(Provide[Container.cloud_tasks_service])
):
    for index, customer in enumerate(emerge_service.get_all_customers()):
        try:
            cloud_tasks_service.enqueue(
                'hubspot/v1/company-sync/worker',
                payload=HubSpotCompanySyncRequest(
                    emerge_company_id=customer.company_id
                ).dict()
            )
        except Exception as e:
            logger.log_text(
                f"Job failed at customer {index + 1}: {customer.company_name} ({customer.company_id}) with the failure:"
                f" {str(e)}",
                severity='DEBUG'
            )


@inject
def get_or_create_hubspot_company_by_name(
    company_name: str,
    hubspot_service: HubSpotService = Depends(Provide[Container.hubspot_service])
):
    companies = hubspot_service.get_company_by_name(
        company_name=company_name
    )
    logger.log_text(
        f"Companies search result for {company_name}: {companies}",
        severity='DEBUG'
    )
    if companies['total'] == 0:
        # create a new company
        logger.log_text(
            f"No Company found in HubSpot with name {company_name}. Creating a new one...",
            severity='DEBUG'
        )

        properties = {
            "name": company_name,
        }
        company = hubspot_service.create_company(properties=properties)
        return company['id']
    elif companies['total'] == 1:
        company = companies['results'][0]
        logger.log_text(
            f"Found Company in HubSpot with name {company_name}: {company['id']}",
            severity='DEBUG'
        )
        return company['id']
    else:
        company = companies['results'][0]
        company_id = company['id']
        logger.log_text(
            f"Multiple companies found with name {company_name}. Merging {companies['total']} companies: {companies}",
            severity='DEBUG'
        )
        for company_to_merge in companies['results'][1:]:
            hubspot_service.merge_companies(
                company_to_merge=company_to_merge['id'],
                company_to_keep=company_id
            )
        return company_id


@inject
def sync_line_items(
    sync_request: HubSpotLineItemSyncRequest,
    hubspot_service: HubSpotService = Depends(Provide[Container.hubspot_service])
):
    deal = hubspot_service.get_deal(
        deal_id=sync_request.object_id,
        associations=['line_item']
    )
    if not sync_request.pricing_tier:
        if deal.get('associations'):
            for association in deal['associations']['line_items']['results']:
                hubspot_service.delete_line_item(line_item_id=association['id'])
    products = hubspot_service.get_all_products(property_names=PRODUCT_PROPERTIES)
    pricing_property = 'price'
    if sync_request.pricing_tier == PricingTier.TIER_2:
        pricing_property = 'tier_2'
    elif sync_request.pricing_tier == PricingTier.TIER_3:
        pricing_property = 'tier_3'

    line_items_to_update = []
    if deal.get('associations'):
        known_product_ids = []
        for association in deal['associations']['line items']['results']:
            line_item = hubspot_service.get_line_item(line_item_id=association['id'])
            product_id = line_item['properties']['hs_product_id']
            if product_id in products.keys() and products[product_id].get(pricing_property):
                # update existing line items with new prices
                known_product_ids.append(product_id)
                amount = products[line_item['properties']['hs_product_id']][pricing_property]
                line_item['properties']['price'] = amount
                line_items_to_update.append(line_item)
            else:
                # delete line items for unknown products or unknown price
                hubspot_service.delete_line_item(line_item_id=line_item['id'])

        hubspot_service.update_line_items(records=line_items_to_update)

        # create missing line items
        for product_id, product in products.items():
            if product_id not in known_product_ids:
                if product.get(pricing_property):
                    properties = {
                        'name': product['name'],
                        'hs_product_id': product_id,
                        'quantity': "1",
                        'price': product[pricing_property]
                    }

                    line_item = hubspot_service.create_line_item(properties=properties)
                    hubspot_service.set_deal_for_line_item(line_item_id=line_item['id'], deal_id=sync_request.object_id)
    else:
        for product_id, product in products.items():
            if product.get(pricing_property):
                properties = {
                    'name': product['name'],
                    'hs_product_id': product_id,
                    'quantity': 1,
                    'price': product[pricing_property]
                }

                line_item = hubspot_service.create_line_item(properties=properties)
                hubspot_service.set_deal_for_line_item(line_item_id=line_item['id'], deal_id=sync_request.object_id)

