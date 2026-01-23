from astropy.wcs import WCS
from astropy.coordinates import SkyCoord

def get_centre_from_header(header):
    """
    Calculate the SkyCoord of the centre of an image from its FITS header.
    """
    w = WCS(header)
    c_ra_pix = (header['NAXIS1'] - 1) / 2
    c_dec_pix = (header['NAXIS2'] - 1) / 2
    centre = SkyCoord.from_pixel(c_ra_pix, c_dec_pix, wcs=w)
    return SkyCoord.from_pixel(c_ra_pix, c_dec_pix, wcs=w)