from datetime import datetime
from typing import Optional

import pydantic
from pydantic import BaseModel


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
    object_id: int
    year: int = datetime.today().year
    month: int = datetime.today().month
    emerge_company_id: Optional[int]


class EmergeSales(BaseModel):
    volume: Optional[int] = pydantic.Field(alias = "Volume")
    sales: Optional[float] = pydantic.Field(alias = "Sales")

    def to_string(self):
        return f"Volume: {self.volume}, Sales: {'${:,.2f}'.format(self.sales)}"


class EmergeProductTypes(BaseModel):
    number_of_packages: Optional[int] = pydantic.Field(alias = "NumberOfPackages")
    number_of_individual_reports: Optional[int] = pydantic.Field(alias = "NumberOfIndividualReports")

    def to_string(self):
        return f"{self.number_of_packages} Packages, {self.number_of_individual_reports} Individual Reports"


class EmergeCompanyInfo(BaseModel):
    company_id: int = pydantic.Field(alias = "EmergeCompanyId")
    company_name: str = pydantic.Field(alias = "EmergeCompanyName")
    hubspot_object_id: Optional[int] = pydantic.Field(alias = "HubSpotObjectId")
    account_status: str = pydantic.Field(alias = "AccountStatus")
    date_opened: datetime = pydantic.Field(alias = "DateOpened")
    number_of_users: int = pydantic.Field(alias = "NumberOfUsers")
    number_of_locations: int = pydantic.Field(alias = "NumberOfLocations")


class EmergeCompanyBillingInfo(BaseModel):
    company_id: Optional[int] = pydantic.Field(alias = "EmergeCompanyId")
    company_name: Optional[str] = pydantic.Field(alias = "EmergeCompanyName")
    account_status: Optional[str] = pydantic.Field(alias = "AccountStatus")
    date_opened: Optional[datetime] = pydantic.Field(alias = "DateOpened")
    number_of_users: Optional[int] = pydantic.Field(alias = "NumberOfUsers")
    number_of_locations: Optional[int] = pydantic.Field(alias = "NumberOfLocations")
    last_report_run: Optional[datetime] = pydantic.Field(alias = "LastReportRun")
    sales_last_month: Optional[EmergeSales] = pydantic.Field(alias = "SalesLastMonth")
    sales_current_month: Optional[EmergeSales] = pydantic.Field(alias = "SalesCurrentMonth")
    sales_ytd: Optional[EmergeSales] = pydantic.Field(alias = "SalesYTD")
    product_types_last_month: Optional[EmergeProductTypes] = pydantic.Field(alias = "ProductsTypeLastMonth")
    product_types_current_month: Optional[EmergeProductTypes] = pydantic.Field(alias = "ProductsTypeCurrentMonth")
    product_types_ytd: Optional[EmergeProductTypes] = pydantic.Field(alias = "ProductsTypeYTD")

    def to_hubspot_company(self):
        change_in_sales = "N/A"
        if (
            self.sales_current_month
            and self.sales_current_month.sales
            and self.sales_last_month
            and self.sales_last_month.sales
        ):
            change_in_sales = (self.sales_current_month.sales - self.sales_last_month.sales) / \
                              self.sales_last_month.sales
            if change_in_sales < 0:
                change_in_sales = f"{'{:,.0f}'.format(change_in_sales)}% 🔻"
            else:
                change_in_sales = f"{'{:,.0f}'.format(change_in_sales)}% ⭐"
        return {
            "name": self.company_name if self.company_name else None,
            "date_opened": int(self.date_opened.timestamp() * 1000) if self.date_opened else None,
            "of_locations": int(self.number_of_locations) if self.number_of_locations else None,
            "company_status": self.account_status.upper() if self.account_status else None,
            "of_users": int(self.number_of_users) if self.number_of_users else None,
            "sales_last_month": self.sales_last_month.sales if self.sales_last_month else None,
            "volume_last_month": self.sales_last_month.volume if self.sales_last_month else None,
            "sales_current_month": self.sales_current_month.sales if self.sales_current_month else None,
            "volume_current_month": self.sales_current_month.volume if self.sales_current_month else None,
            "sales_ytd": self.sales_ytd.sales if self.sales_ytd else None,
            "volume_ytd": self.sales_ytd.volume if self.sales_ytd else None,
            "change_in_sales": change_in_sales,
            "product_types_last_month": self.product_types_last_month.to_string() if self.product_types_last_month
            else None,
            "product_types_current_month": self.product_types_current_month.to_string() if
            self.product_types_current_month else None,
            "product_types_ytd": self.product_types_ytd.to_string() if self.product_types_ytd else None,
            "last_report_run": int(self.last_report_run.timestamp() * 1000) if self.last_report_run else None
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
                if change_in_sales < 0:
                    change_in_sales = f"{'{:,.0f}'.format(change_in_sales)}% 🔻"
                else:
                    change_in_sales = f"{'{:,.0f}'.format(change_in_sales)}% ⭐"
            data = {
                "objectId": self.company_id,
                "title": self.company_name,
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
