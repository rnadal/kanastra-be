from datetime import date
from decimal import Decimal
from uuid import UUID
from pydantic import BaseModel, EmailStr, Field, field_validator
import re

class ChargeNotification(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    government_id: str = Field(..., min_length=11, max_length=14)
    email: EmailStr
    debt_amount: Decimal = Field(..., gt=0)
    debt_due_date: date
    debt_id: UUID