class WallabyPipelineError(RuntimeError):
    """Base class for all WALLABY Milky Way pipeline errors."""


class CasdaError(WallabyPipelineError):
    """Base class for CASDA-related failures."""


class CasdaAuthError(CasdaError):
    """CASDA authentication failed."""


class CasdaStagingError(CasdaError):
    """CASDA staging or download failed."""


class CasdaTapJobError(CasdaError):
    """TAP job failed, timed out, or did not complete."""