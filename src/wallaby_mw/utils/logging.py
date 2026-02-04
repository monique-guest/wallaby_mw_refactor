import logging
import warnings

from astropy.utils.exceptions import AstropyWarning


def setup_logging(level: str) -> None:
    """
    Configure root logging and quiet noisy third-party libraries by default.
    """
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    if level != "DEBUG":
        logging.getLogger("astroquery").setLevel(logging.WARNING)
        logging.getLogger("astropy").setLevel(logging.WARNING)

    # Silence VOTable unit warnings from CASDA datalink ("pixel" unit)
    warnings.filterwarnings(
        "ignore",
        message=r".*Invalid unit string 'pixel'.*",
        category=AstropyWarning,
    )
