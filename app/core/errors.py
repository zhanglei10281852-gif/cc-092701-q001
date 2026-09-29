from __future__ import annotations


class DomainError(Exception):
    status_code = 400
    code = "domain_error"

    def __init__(self, message: str, *, context: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context = context or {}


class NotFoundError(DomainError):
    status_code = 404
    code = "not_found"


class ConflictError(DomainError):
    status_code = 409
    code = "conflict"


class LeaseExpiredError(ConflictError):
    """凭证本身正确，但租约已经超过到期时刻，旧会话不得自救。"""

    code = "lease_expired"


class LeaseLostError(ConflictError):
    """租约已被恢复流程撤销或被另一个会话接管，凭证不再有效。"""

    code = "lease_lost"


class VersionConflictError(ConflictError):
    """租约仍在，但调用方持有的任务版本已经陈旧。"""

    code = "version_conflict"


class TaskClosedError(ConflictError):
    """任务已进入终态，迟到的回执不能再改变结果。"""

    code = "task_closed"


class ReceiptConflictError(ConflictError):
    """同一回执键被用于内容不同的请求。"""

    code = "receipt_conflict"


class AuthenticationError(DomainError):
    status_code = 401
    code = "authentication_failed"


class PermissionDeniedError(DomainError):
    status_code = 403
    code = "permission_denied"


class ValidationError(DomainError):
    status_code = 422
    code = "validation_error"


class AccountLockedError(AuthenticationError):
    code = "account_locked"


class SessionExpiredError(AuthenticationError):
    code = "session_expired"
