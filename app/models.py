from datetime import datetime
from enum import Enum
from typing import List, Optional, Any

from pydantic import BaseModel, Field


class PandadocProposalRequest(BaseModel):
    email: str
    first_name: str
    last_name: str
    company_name: str
    deal_name: str
    package_1_price: str
    package_2_price: str
    package_3_price: str
    prepared_by: str
    background_check_line_items: Optional[List[Any]]
    drug_test_line_items: Optional[List[Any]]


class HubSpotWebhookEvent(BaseModel):
    objectId: int
    propertyName: str
    propertyValue: str
    changeSource: str
    eventId: int
    subscriptionId: int
    portalId: int
    appId: int
    occurredAt: int
    subscriptionType: str
    attemptNumber: int


class HubSpotCompanySyncRequest(BaseModel):
    object_id: Optional[int]
    type: str
    year: int = datetime.today().year
    month: int = datetime.today().month
    emerge_company_id: Optional[int]
    days_from_last_report: Optional[int]
    account_manager_email: Optional[str]
    status_change_date: Optional[int]


class HubSpotDealSyncRequest(BaseModel):
    object_id: int


class PricingTier(str, Enum):
    TIER_1 = "A"
    TIER_2 = "B"
    TIER_3 = "C"


class HubSpotLineItemSyncRequest(BaseModel):
    object_id: int
    pricing_tier: Optional[PricingTier]


class HubSpotAssociation(BaseModel):
    id: str
    type: str


class HubSpotAssociationFrom(BaseModel):
    id: str


class HubSpotAssociationResult(BaseModel):
    from_object: HubSpotAssociationFrom = Field(alias="from")
    to: List[HubSpotAssociation]

    def first(self):
        return self.to.pop(0) if len(self.to) > 0 else None


class HubSpotAssociationBatchReadResponse(BaseModel):
    status: str
    results: List[HubSpotAssociationResult]
    started_at: datetime
    completed_at: datetime

    def first(self):
        return self.results[0].to[0] if len(self.results) > 0 and len(self.results[0].to) > 0 else None


class EmergeSales(BaseModel):
    volume: Optional[int] = Field(alias="Volume", default=None)
    sales: Optional[float] = Field(alias="Sales", default=None)

    def to_string(self):
        return f"Volume: {self.volume}{' ⭐' if self.volume > 499 else ''}, Sales: {'${:,.2f}'.format(self.sales)}"


class EmergeProductTypes(BaseModel):
    number_of_packages: Optional[int] = Field(alias="NumberOfPackages", default=None)
    number_of_individual_reports: Optional[int] = Field(alias="NumberOfIndividualReports", default=None)

    def to_string(self):
        return f"{self.number_of_packages} Packages, {self.number_of_individual_reports} Individual Reports"


class EmergeCompanyInfo(BaseModel):
    company_id: int = Field(alias="EmergeCompanyId")
    company_name: str = Field(alias="EmergeCompanyName")
    hubspot_object_id: Optional[int] = Field(alias="HubSpotObjectId")
    account_status: str = Field(alias="AccountStatus")
    date_opened: datetime = Field(alias="DateOpened")
    number_of_users: int = Field(alias="NumberOfUsers")
    number_of_locations: int = Field(alias="NumberOfLocations")
    days_from_last_report: Optional[int] = Field(alias="DaysFromLastReport", default=None)
    account_manager_email: Optional[str] = Field(alias="AccountManagerEmail", default=None)
    status_change_date: Optional[datetime] = Field(alias="StatusChangeDate", default=None)
    last_modified_date: Optional[datetime] = Field(alias="LastModifiedDate", default=None)


class EmergeCompanyBillingInfo(BaseModel):
    company_id: Optional[int] = Field(alias="EmergeCompanyId", default=None)
    company_name: Optional[str] = Field(alias="EmergeCompanyName", default=None)
    account_status: Optional[str] = Field(alias="AccountStatus", default=None)
    date_opened: Optional[datetime] = Field(alias="DateOpened", default=None)
    number_of_users: Optional[int] = Field(alias="NumberOfUsers", default=None)
    number_of_locations: Optional[int] = Field(alias="NumberOfLocations", default=None)
    last_report_run: Optional[datetime] = Field(alias="LastReportRun", default=None)
    sales_last_month: Optional[EmergeSales] = Field(alias="SalesLastMonth", default=None)
    sales_current_month: Optional[EmergeSales] = Field(alias="SalesCurrentMonth", default=None)
    sales_ytd: Optional[EmergeSales] = Field(alias="SalesYTD", default=None)
    product_types_last_month: Optional[EmergeProductTypes] = Field(alias="ProductsTypeLastMonth", default=None)
    product_types_current_month: Optional[EmergeProductTypes] = Field(alias="ProductsTypeCurrentMonth", default=None)
    product_types_ytd: Optional[EmergeProductTypes] = Field(alias="ProductsTypeYTD", default=None)

    def to_hubspot_company(
        self,
        days_from_last_report: Optional[int] = None,
        owner_id: Optional[int] = None,
        status_change_date: Optional[int] = None
    ):
        change_in_sales = "N/A"
        if (
            self.sales_current_month
            and self.sales_last_month
            and self.sales_last_month.sales > 0
        ):
            change_in_sales = (self.sales_current_month.sales - self.sales_last_month.sales) / \
                              self.sales_last_month.sales * 100
            if change_in_sales < -20:
                change_in_sales = f"{'{:,.0f}'.format(change_in_sales)}% 🔻"
            elif change_in_sales > 20:
                change_in_sales = f"{'{:,.0f}'.format(change_in_sales)}% ▲"
            else:
                change_in_sales = f"{'{:,.0f}'.format(change_in_sales)}%"

        change_in_volume = "N/A"
        company_name = self.company_name.strip(' ⭐') if self.company_name else None
        if (
            self.sales_current_month
            and self.sales_last_month
            and self.sales_last_month.volume > 0
        ):
            change_in_volume = (self.sales_current_month.volume - self.sales_last_month.volume) / \
                               self.sales_last_month.volume * 100
            if change_in_volume < -20:
                change_in_volume = f"{'{:,.0f}'.format(change_in_volume)}% 🔻"
            elif change_in_volume > 20:
                change_in_volume = f"{'{:,.0f}'.format(change_in_volume)}% ▲"
            else:
                change_in_volume = f"{'{:,.0f}'.format(change_in_volume)}%"

            change_in_volume = f"{self.sales_current_month.volume} | {self.sales_last_month.volume} |" \
                               f" {change_in_volume}"

            if self.sales_current_month.volume > 499:
                company_name = f"{company_name} ⭐"
        return {
            "name": company_name,
            "emerge_company_id": self.company_id,
            "date_opened": int(self.date_opened.timestamp() * 1000) if self.date_opened else None,
            "of_locations": int(self.number_of_locations) if self.number_of_locations else 0,
            "company_status": self.account_status.upper() if self.account_status else None,
            "of_users": int(self.number_of_users) if self.number_of_users else 0,
            "sales_last_month": self.sales_last_month.sales if self.sales_last_month else 0,
            "volume_last_month": self.sales_last_month.volume if self.sales_last_month else 0,
            "sales_current_month": self.sales_current_month.sales if self.sales_current_month else 0,
            "volume_current_month": self.sales_current_month.volume if self.sales_current_month else 0,
            "sales_ytd": self.sales_ytd.sales if self.sales_ytd else 0,
            "volume_ytd": self.sales_ytd.volume if self.sales_ytd else 0,
            "change_in_sales": change_in_sales,
            "change_in_volume": change_in_volume,
            "product_types_last_month": self.product_types_last_month.to_string() if self.product_types_last_month
            else None,
            "product_types_current_month": self.product_types_current_month.to_string() if
            self.product_types_current_month else None,
            "product_types_ytd": self.product_types_ytd.to_string() if self.product_types_ytd else None,
            "last_report_run": int(self.last_report_run.timestamp() * 1000) if self.last_report_run else None,
            "customer_deal_stages_sync": True,
            "days_from_last_report": days_from_last_report,
            "hubspot_owner_id": owner_id,
            "last_status_change_date": status_change_date
        }

    def to_hubspot_crm_card(self):
        results = []
        if self.company_id:
            change_in_sales = "N/A"
            if (
                self.sales_current_month
                and self.sales_last_month
                and self.sales_last_month.sales > 0
            ):
                change_in_sales = (self.sales_current_month.sales - self.sales_last_month.sales) / \
                                  self.sales_last_month.sales * 100
                if change_in_sales < -20:
                    change_in_sales = f"{'{:,.0f}'.format(change_in_sales)}% 🔻"
                elif change_in_sales > 20:
                    change_in_sales = f"{'{:,.0f}'.format(change_in_sales)}% ▲"
                else:
                    change_in_sales = f"{'{:,.0f}'.format(change_in_sales)}%"

            change_in_volume = "N/A"
            company_name = self.company_name.strip(' ⭐') if self.company_name else None
            if (
                self.sales_current_month
                and self.sales_last_month
                and self.sales_last_month.volume > 0
            ):
                change_in_volume = (self.sales_current_month.volume - self.sales_last_month.volume) / \
                                   self.sales_last_month.volume * 100
                if change_in_volume < -20:
                    change_in_volume = f"{'{:,.0f}'.format(change_in_volume)}% 🔻"
                elif change_in_volume > 20:
                    change_in_volume = f"{'{:,.0f}'.format(change_in_volume)}% ▲"
                else:
                    change_in_volume = f"{'{:,.0f}'.format(change_in_volume)}%"

                change_in_volume = f"{self.sales_current_month.volume} | {self.sales_last_month.volume} |" \
                                   f" {change_in_volume}"

                if self.sales_current_month.volume > 499:
                    company_name = f"{company_name} ⭐"
            data = {
                "objectId": self.company_id,
                "title": company_name,
                "link": f"https://emerge.intelifi.com/companies/{self.company_id}",
                "date_opened": int(self.date_opened.timestamp() * 1000) if self.date_opened else None,
                "number_of_locations": int(self.number_of_locations) if self.number_of_locations else None,
                "account_status": self.account_status.upper() if self.account_status else None,
                "number_of_users": int(self.number_of_users) if self.number_of_users else None,
                "sales_last_month": self.sales_last_month.to_string() if self.sales_last_month else None,
                "sales_current_month": self.sales_current_month.to_string() if self.sales_current_month else None,
                "sales_ytd": self.sales_ytd.to_string() if self.sales_ytd else None,
                "volume_ytd": self.sales_ytd.volume if self.sales_ytd else None,
                "change_in_sales": change_in_sales,
                "change_in_volume": change_in_volume,
                "product_types_last_month": self.product_types_last_month.to_string() if self.product_types_last_month
                else None,
                "product_types_current_month": self.product_types_current_month.to_string() if
                self.product_types_current_month else None,
                "product_types_ytd": self.product_types_ytd.to_string() if self.product_types_ytd else None,
                "last_report_run": int(self.last_report_run.timestamp() * 1000) if self.last_report_run else None
            }
            results.append({k: v for k, v in data.items() if v is not None})

        return {
            'results': results
        }
