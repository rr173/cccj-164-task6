"""多租户 API 配额租约服务。"""

from .domain import DomainError
from .service import QuotaService

__all__ = ["QuotaService", "DomainError"]
