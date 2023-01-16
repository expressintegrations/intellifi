from datetime import datetime

from dependency_injector.wiring import inject, Provide
from fastapi import Depends
from google.cloud import logging

from .containers import Container
from .models import HubSpotCompanySyncRequest, HubSpotDealSyncRequest, HubSpotLineItemSyncRequest, PricingTier, \
    PandadocProposalRequest
from .services import CloudTasksService, EmergeService, HubSpotService, PandadocService, FirestoreService

log_name = 'intellifi.functions'
logging_client = logging.Client()
logger = logging_client.logger(log_name)

PRODUCT_PROPERTIES = ['name', 'price', 'tier_2', 'tier_3', 'hs_product_id', 'hs_sku']
LINE_ITEM_PROPERTIES = ['hs_product_id', 'price', 'hs_sku']


@inject
def get_pandadoc_proposal_session(
    pandadoc_proposal_request: PandadocProposalRequest,
    pandadoc_service: PandadocService = Depends(Provide[Container.pandadoc_service])
):
    return pandadoc_service.get_proposal_session(pandadoc_proposal_request=pandadoc_proposal_request)


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
    logger.log_text(
        f"Syncing Emerge Company {emerge_company.json()}",
        severity='DEBUG'
    )
    hubspot_company_id = None
    if hubspot_company_sync_request.object_id:
        if hubspot_company_sync_request.type == 'COMPANY':
            hubspot_company_id = hubspot_company_sync_request.object_id
        if hubspot_company_sync_request.type == 'DEAL':
            associations = hubspot_service.get_company_for_deal(
                deal_id=hubspot_company_sync_request.object_id
            )
            hubspot_company_id = associations.first().id if associations.first() else None
    if not hubspot_company_id:
        companies = hubspot_service.get_company_by_emerge_company(
            emerge_company_id=hubspot_company_sync_request.emerge_company_id
        )
        logger.log_text(
            f"Companies search result for {hubspot_company_sync_request.emerge_company_id}: {companies}",
            severity='DEBUG'
        )
        if companies['total'] == 0:
            if not hubspot_company_sync_request.object_id:
                logger.log_text(
                    (
                        f"Unable to locate a company in HubSpot by Emerge Company ID"
                        f" {hubspot_company_sync_request.emerge_company_id}. Skipping..."
                    ),
                    severity='DEBUG'
                )
                return
            c_association = hubspot_service.get_company_for_deal(hubspot_company_sync_request.object_id).results[0]
            hubspot_company_id = c_association['to'][0]['id']
            logger.log_text(
                f"No Company found in HubSpot with Emerge Company ID {hubspot_company_sync_request.emerge_company_id}. "
                f"Located company {hubspot_company_id} by deal ID {hubspot_company_sync_request.object_id} in HubSpot",
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
    owner_id = hubspot_service.get_owner_by_email(email=hubspot_company_sync_request.account_manager_email)
    update_result = hubspot_service.update_company(
        company_id=hubspot_company_id,
        properties=emerge_company.to_hubspot_company(
            days_from_last_report=hubspot_company_sync_request.days_from_last_report,
            owner_id=owner_id,
            status_change_date=hubspot_company_sync_request.status_change_date
        )
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
    cloud_tasks_service: CloudTasksService = Depends(Provide[Container.cloud_tasks_service]),
    firestore_service: FirestoreService = Depends(Provide[Container.firestore_service]),
    force: bool = False
):
    start_time = datetime.now()
    last_run_date = firestore_service.get_emerge_sync_last_run_date()
    if force:
        last_run_date = '01-01-2000'
    logger.log_text(
        f"Checking for records updated since {last_run_date}...",
        severity='DEBUG'
    )
    firestore_service.set_emerge_sync_last_run_date(last_run_date=start_time.strftime('%m-%d-%Y'))
    for index, customer in enumerate(emerge_service.get_all_customers(since=last_run_date)):
        try:
            scd = int(customer.status_change_date.timestamp() * 1000) if customer.status_change_date else None
            cloud_tasks_service.enqueue(
                'hubspot/v1/company-sync/worker',
                payload=HubSpotCompanySyncRequest(
                    emerge_company_id=customer.company_id,
                    type='DEAL',
                    object_id=customer.hubspot_object_id,
                    days_from_last_report=customer.days_from_last_report,
                    account_manager_email=customer.account_manager_email,
                    status_change_date=scd
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
        company_name=company_name.strip()
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
            line_item_ids = [association['id'] for association in deal['associations']['line items']['results']]
            hubspot_service.delete_line_items(line_item_ids=line_item_ids)
            return
    products = hubspot_service.get_all_products(property_names=PRODUCT_PROPERTIES)
    pricing_property = 'price'
    if sync_request.pricing_tier == PricingTier.TIER_2:
        pricing_property = 'tier_2'
    elif sync_request.pricing_tier == PricingTier.TIER_3:
        pricing_property = 'tier_3'

    line_items_to_update = []
    line_items_to_delete = []
    line_items_to_create = []
    if deal.get('associations'):
        known_product_ids = []
        line_item_ids = [association['id'] for association in deal['associations']['line items']['results']]
        deal_line_items = hubspot_service.get_line_items(line_item_ids=line_item_ids)
        for line_item in deal_line_items:
            product_id = line_item['properties']['hs_product_id']
            if (
                product_id in products.keys()
                and products[product_id].get(pricing_property)
                and products[product_id].get('hs_sku')
            ):
                # update existing line items with new prices
                known_product_ids.append(product_id)
                amount = products[line_item['properties']['hs_product_id']][pricing_property]
                line_item['properties']['price'] = amount
                line_items_to_update.append(line_item)
            else:
                # delete line items for unknown products or unknown price
                line_items_to_delete.append(line_item['id'])

        if len(line_items_to_update) > 0:
            hubspot_service.update_line_items(records=line_items_to_update)

        if len(line_items_to_delete) > 0:
            hubspot_service.delete_line_items(line_item_ids=line_items_to_delete)

        # create missing line items
        for product_id, product in products.items():
            if product_id not in known_product_ids:
                if product.get(pricing_property) and product.get('hs_sku'):
                    properties = {
                        'name': product['name'],
                        'hs_product_id': product_id,
                        'quantity': "1",
                        'price': product[pricing_property]
                    }
                    line_items_to_create.append(properties)
    else:
        for product_id, product in products.items():
            if product.get(pricing_property) and product.get('hs_sku'):
                properties = {
                    'name': product['name'],
                    'hs_product_id': product_id,
                    'quantity': 1,
                    'price': product[pricing_property]
                }
                line_items_to_create.append(properties)

    if len(line_items_to_create) > 0:
        line_items = hubspot_service.create_line_items(line_items=line_items_to_create)
        hubspot_service.set_deal_for_line_items(
            line_items=line_items,
            deal_id=sync_request.object_id
        )
