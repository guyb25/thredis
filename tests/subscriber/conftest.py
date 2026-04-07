from pydantic import BaseModel


class OrderMessage(BaseModel):
    id: str
    amount: float
