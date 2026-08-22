from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID
from pydantic import BaseModel, Field, EmailStr

class PriorityEnum(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"

# Map string priority to database integer representation
PRIORITY_MAP = {
    PriorityEnum.LOW: 0,
    PriorityEnum.NORMAL: 1,
    PriorityEnum.HIGH: 2,
    PriorityEnum.CRITICAL: 3,
}

# Reverse mapping for responses
REVERSE_PRIORITY_MAP = {v: k for k, v in PRIORITY_MAP.items()}

class JobStatusEnum(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    SCHEDULED = "scheduled"
    DEAD_LETTERED = "dead_lettered"


# User Schemas
class UserRegister(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6, max_length=100)

class Token(BaseModel):
    access_token: str
    token_type: str

class UserResponse(BaseModel):
    id: UUID
    username: str
    created_at: datetime

    class Config:
        from_attributes = True


# Job Schemas
class JobCreate(BaseModel):
    type: str = Field(..., min_length=1, max_length=50, description="Job type, e.g., 'sleep', 'cpu_bound'")
    payload: dict[str, Any] = Field(default_factory=dict, description="Arbitrary job payload arguments")
    priority: PriorityEnum = Field(default=PriorityEnum.NORMAL, description="Job execution priority")
    max_attempts: int = Field(default=3, ge=1, le=10, description="Maximum number of retry attempts")
    timeout: int = Field(default=60, ge=5, le=3600, description="Execution timeout in seconds")
    scheduled_at: Optional[datetime] = Field(default=None, description="Future timestamp for scheduled execution")

class JobAttemptResponse(BaseModel):
    id: UUID
    attempt_number: int
    status: str
    worker_id: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None
    error_info: Optional[dict[str, Any]] = None

    class Config:
        from_attributes = True

class JobResponse(BaseModel):
    id: UUID
    user_id: UUID
    type: str
    payload: dict[str, Any]
    status: JobStatusEnum
    priority: PriorityEnum
    attempt_count: int
    max_attempts: int
    progress: int
    result: Optional[dict[str, Any]] = None
    error_info: Optional[dict[str, Any]] = None
    idempotency_key: Optional[str] = None
    timeout: int
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    attempts: list[JobAttemptResponse] = Field(default_factory=list)

    @classmethod
    def model_validate(cls, obj: Any, *args, **kwargs) -> "JobResponse":
        # Convert SQLAlchemy object to dict to customize mapping
        if not isinstance(obj, dict):
            # Create a dict from the attributes of the SQLAlchemy model
            data = {}
            for field in cls.model_fields:
                if field == "priority" and hasattr(obj, "priority"):
                    db_priority = getattr(obj, "priority")
                    data["priority"] = REVERSE_PRIORITY_MAP.get(db_priority, PriorityEnum.NORMAL)
                elif field == "status" and hasattr(obj, "status"):
                    data["status"] = JobStatusEnum(getattr(obj, "status"))
                elif field == "attempts":
                    if "attempts" in obj.__dict__:
                        data["attempts"] = [JobAttemptResponse.model_validate(a) for a in obj.attempts]
                    else:
                        data["attempts"] = []
                elif hasattr(obj, field):
                    data[field] = getattr(obj, field)
            return super().model_validate(data, *args, **kwargs)
        return super().model_validate(obj, *args, **kwargs)

    class Config:
        from_attributes = True

class JobListResponse(BaseModel):
    items: list[JobResponse]
    total: int
    limit: int
    offset: int


# Error Schemas
class ErrorDetail(BaseModel):
    code: str
    message: str
    request_id: Optional[str] = None

class ErrorResponse(BaseModel):
    error: ErrorDetail
