"""Pydantic Schema Definitions for CDC Events.

Validates CDC event headers (event_id, customer_id, operation, event_time, source_sequence)
and customer payload attributes according to the database schema.
Preserves extra metadata (_record_type, _test_category) for end-to-end traceability.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class OperationType(str, Enum):
    """Permitted CDC operation types."""

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class CustomerPayload(BaseModel):
    """Customer entity attributes schema matching PostgreSQL schema constraints."""

    model_config = ConfigDict(extra="allow")

    customer_id: Optional[str] = Field(default=None, max_length=64)
    customer_name: Optional[str] = Field(default=None, max_length=255)
    email: Optional[str] = Field(default=None, max_length=255)
    city: Optional[str] = Field(default=None, max_length=255)


class CDCEvent(BaseModel):
    """CDC Customer Event Model with strict validation.

    Ensures:
    - event_id is a valid RFC 4122 UUID.
    - customer_id is provided and within length bounds.
    - operation is strictly INSERT, UPDATE, or DELETE.
    - event_time parses to a valid datetime.
    - source_sequence is a non-negative integer.
    - INSERT operations require customer_name and email in the payload.
    - UPDATE operations support partial attribute payloads.
    - Extra metadata is preserved without silent loss.
    """

    model_config = ConfigDict(extra="allow")

    event_id: UUID = Field(..., description="Unique UUID identifier for the CDC event")
    customer_id: str = Field(..., min_length=1, max_length=64, description="Target customer identifier")
    operation: OperationType = Field(..., description="CDC operation: INSERT, UPDATE, or DELETE")
    event_time: datetime = Field(..., description="Timestamp when the mutation occurred in the source")
    source_sequence: int = Field(..., ge=0, description="Monotonically increasing sequence number >= 0")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Customer attribute values or delta")

    @field_validator("customer_id")
    @classmethod
    def validate_customer_id_non_empty(cls, v: str) -> str:
        """Ensure customer_id is not only whitespace."""
        stripped = v.strip()
        if not stripped:
            raise ValueError("customer_id cannot be empty or blank")
        return stripped

    @model_validator(mode="after")
    def validate_operation_specific_payload(self) -> "CDCEvent":
        """Validate payload structure according to operation type."""
        op = self.operation

        if op == OperationType.INSERT:
            # INSERT requires customer_name and email
            name = self.payload.get("customer_name")
            email = self.payload.get("email")

            if not name or not str(name).strip():
                raise ValueError("INSERT event payload must contain a non-empty 'customer_name'")
            if len(str(name)) > 255:
                raise ValueError("customer_name exceeds maximum length of 255 characters")

            if not email or not str(email).strip():
                raise ValueError("INSERT event payload must contain a non-empty 'email'")
            if len(str(email)) > 255:
                raise ValueError("email exceeds maximum length of 255 characters")

            city = self.payload.get("city")
            if city is not None and len(str(city)) > 255:
                raise ValueError("city exceeds maximum length of 255 characters")

        elif op == OperationType.UPDATE:
            # UPDATE supports partial attributes; validate lengths if present
            name = self.payload.get("customer_name")
            if name is not None:
                if not str(name).strip():
                    raise ValueError("customer_name in UPDATE cannot be empty string")
                if len(str(name)) > 255:
                    raise ValueError("customer_name exceeds maximum length of 255 characters")

            email = self.payload.get("email")
            if email is not None:
                if not str(email).strip():
                    raise ValueError("email in UPDATE cannot be empty string")
                if len(str(email)) > 255:
                    raise ValueError("email exceeds maximum length of 255 characters")

            city = self.payload.get("city")
            if city is not None and len(str(city)) > 255:
                raise ValueError("city exceeds maximum length of 255 characters")

        elif op == OperationType.DELETE:
            # DELETE payload can be minimal (e.g. {"customer_id": ...})
            pass

        return self

