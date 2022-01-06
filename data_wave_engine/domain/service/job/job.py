from pydantic import BaseModel
class JobActivateRequest(BaseModel):
    job_id: str

