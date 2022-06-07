from pydantic import BaseModel


class HubSpotCompaniesRequest(BaseModel):
  userId: int
  userEmail: str
