"""Shared domain-level service exceptions."""


class ServiceError(Exception):
    """Base class for domain/service failures that map to HTTP responses."""

    status_code = 500

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class BadRequestError(ServiceError):
    status_code = 400


class UnauthorizedError(ServiceError):
    status_code = 401


class ForbiddenError(ServiceError):
    status_code = 403


class NotFoundError(ServiceError):
    status_code = 404


class ConflictError(ServiceError):
    status_code = 409


class UpstreamServiceError(ServiceError):
    status_code = 502

