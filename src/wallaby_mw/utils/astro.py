from astropy.wcs import WCS
from astropy.coordinates import SkyCoord
import astropy.units as u
import numpy as np

def get_centre_from_header(header):
    """
    Calculate the SkyCoord of the centre of an image from its FITS header.
    """
    w = WCS(header)
    c_ra_pix = (header['NAXIS1'] - 1) / 2
    c_dec_pix = (header['NAXIS2'] - 1) / 2
    centre = SkyCoord.from_pixel(c_ra_pix, c_dec_pix, wcs=w)
    return centre


def wallaby_pixel_region(header, size_arcmin: int) -> tuple[int, int, int, int]:
    """
    Define a square on-sky region of side length `size_arcmin` (arcmin),
    centered on the cube center, returning MIRIAD boxes() pixel coords.

    Returns: (x1, y1, x2, y2) in 1-based inclusive pixel coordinates (MIRIAD).
    """
    w = WCS(header)
    centre = get_centre_from_header(header)

    dr = (size_arcmin / 2) * u.arcmin

    # True angular offsets on the sphere (no cos(dec) distortion)
    bl = centre.spherical_offsets_by(-dr, -dr)  # bottom-left (west, south)
    tr = centre.spherical_offsets_by(+dr, +dr)  # top-right (east, north)

    x1, y1 = bl.to_pixel(w)
    x2, y2 = tr.to_pixel(w)

    xmin, xmax = sorted([x1, x2])
    ymin, ymax = sorted([y1, y2])

    # Convert from astropy 0-based pixels to MIRIAD 1-based pixels
    x1_m = int(np.floor(xmin)) + 1
    y1_m = int(np.floor(ymin)) + 1
    x2_m = int(np.floor(xmax)) + 1
    y2_m = int(np.floor(ymax)) + 1

    return x1_m, y1_m, x2_m, y2_m
