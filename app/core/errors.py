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


class LeaseConflictError(ConflictError):
    """回执出示的租约代次或任务版本已经过期（典型的失联旧会话迟到写入）。"""

    code = "lease_conflict"


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
