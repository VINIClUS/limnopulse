class LimnopulseError(Exception):
    """Base application error."""


class AuthError(LimnopulseError):
    """Identity could not be authenticated."""


class AuthorizationError(LimnopulseError):
    """Identity is authenticated but not authorized for the requested tenant/action."""


class NotFoundError(LimnopulseError):
    """Requested resource was not found."""


class ConflictError(LimnopulseError):
    """Conditional write or version conflict."""


class TelemetryQueryError(LimnopulseError):
    """Telemetry backend could not satisfy a query."""
