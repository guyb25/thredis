"""Shared models used across example services."""

from pydantic import BaseModel


class Order(BaseModel):
    order_id: str
    customer: str
    amount: float
    items: list[str]


class Invoice(BaseModel):
    order_id: str
    customer: str
    total: float
    pdf_url: str


class Notification(BaseModel):
    recipient: str
    subject: str
    body: str
