#!/usr/bin/env python3

# Author: Lister Staveley-Smith
# Date: 12 March 2021
# Update 17 Jan 2022 (python3, WCS and PC support)
# Update 13 Jun 2022 (skip factor bug fix)


# Notes: on Pawsey, firstly execute the command line: "module load python astropy numpy"

# python2 print compatibility
from __future__ import print_function

# Description
Purpose = "Make a subimage of a large multi-dimensional FITS file without reading into memory. Will not overwrite existing file. Will only itemize input FITS file if no output specified."
Conventions = "Pixels start at 1, following FITS convention for CRPIX. Specify a start and end pixel OR start and end world coordinate for each dimension in the order in which the dimensions appear in the FITS file (e.g. min,max,min,max,...). Unspecified min/max or min/max=0 means default to min/max of that dimension. "
Useage = "subfits -i <input file> -o <output file> -p/w <x1,x2,y1,y2,z1,z2> -s <dx,dy,dz> -r OR subfits --in=<input file> --o=<output file> --pix/world=<x1,x2,y1,y2,z1,z2> --skip=<dx,dy,dz> -r"

# packages
import sys
import warnings
from astropy.io import fits
from astropy.utils.exceptions import AstropyWarning
from astropy.wcs import WCS
#import astropy.wcs
import numpy as np
import os
import getopt


def main(argv):
    # maximum dimensions (formatting of PC array may be problematic above this)
    nmax = 9

    # default parameters
    image = "image.fits"
    output = ""
    pixels = "0,0,0,0,0,0"
    world = "0,0,0,0,0,0"
    skip = "0,0,0"
    history = ""
    dummy = False
    pixels_init = pixels
    world_init = world

    # command line options
    optionlist = ['?','in=', 'out=', 'pix=', 'remove', 'skip=', 'world=']
    optionhelp = ['help', 'input file (FITS format)', 'output file', 'pixels (e.g. 0,100,0,100,0,10)', 'remove dummy axes', 'skip (e.g. 2,2,2)', 'world (e.g. 120.0,100.0,-20,-10.0,0,0)']

    try:
        options, remainder = getopt.getopt(argv, '?i:o:p:rs:w:', optionlist)
        history = " ".join(sys.argv)
    except getopt.error as err:
        print (str(err))
        print('OPTION LIST:', optionlist)
        print("Use -? option for help")
        sys.exit(2)

    for opt, arg in options:
        if opt in ('-?', '--?'):      # help
            print("\nPurpose:", Purpose, "\n")
            print("Conventions:", Conventions, "\n")
            print("Limits: maximum number of dimensions =", nmax, "\n")
            print("Useage:", Useage, "\n")
            print("OPTION   DESCRIPTION")
            for i in range(len(optionlist)):
                print("{:8s} {}".format(optionlist[i], optionhelp[i]))
            sys.exit(0)
        elif opt in ('-i', '--in'):
            image = arg
        elif opt in ('-o', '--out'):
            output = arg
        elif opt in ('-p', '--pix'):
            pixels = arg
        elif opt in ('-r', '--remove'):
            dummy = True
        elif opt in ('-s', '--skip'):
            skip = arg
        elif opt in ('-w', '--world'):
            world = arg

    if len(remainder) != 0:
        print('Unrecognised arguments:', remainder)

    # parameter checking
    goworld = False
    if (pixels != pixels_init) and (world != world_init):
        exit("ERROR: cannot use pix (p) and world (w) arguments simultaneously")
    if world != world_init:
        goworld = True

    # Decode pixel limits
    try:
        r = [int(x) for x in pixels.split(",")]
        pdim = len(r)
    except:
        exit("ERROR: invalid format for pixels - use \"-p 0,100,0,100,0,10\" or similar")

    if (pdim % 2) != 0:
        exit("ERROR: even number of pixel coordinates expected")

    # Decode world coordinate limits
    try:
        w = [float(x) for x in world.split(",")]
        wdim = len(w)
    except:
        exit("ERROR: invalid format for world - use \"-w 120.0,100.0,-20,-10.0,0,0\" or similar")

    if (wdim % 2) != 0:
        exit("ERROR: even number of world coordinates expected")

    # Decode skip factors
    try:
        s = [int(x) for x in skip.split(",")]
        sdim = len(s)
    except:
        exit("ERROR: invalid format for skip - use \"-s 2,2,2\" or similar")

    # Summarise arguments
    print("Input  FITS file:", image)
    print("Output FITS file:", output)

    # parameter checking
    if os.path.exists(image) != True:
        exit("ERROR: input file does not exist")
    if output != "":
        if os.path.exists(output):
            exit("ERROR: output file already exists")

    # input FITS file
    try:
        hdu = fits.open(image, memmap=True, lazy_load_hdus=True, do_not_scale_image_data=True, mode='denywrite')
        hdu.info()
    #    print(hdu[0].header)
    except:
        exit("ERROR: unable to read input file")

    # extract more info
    try:
        ndim = hdu[0].header['NAXIS']
        d = [0]*ndim
        print("{} dimensions present:".format(ndim))
        for i in range(ndim):
            d[i] = hdu[0].header['NAXIS'+str(i+1)]
            print(hdu[0].header['CTYPE'+str(i+1)], d[i], "pixels")
    except:
        exit("ERROR: unable to read input file")

    # dimensionality check
    if ndim > nmax:
        exit("ERROR: too many dimensions")
    rdim = [0]*ndim

    # Change pixel/skip/world dimensions to match image
    if pdim > 2*ndim:
        del r[2*ndim:pdim]
    if wdim > 2*ndim:
        del w[2*ndim:wdim]
    if sdim > ndim:
        del s[ndim:sdim]
    if pdim > 2*ndim:
        del r[2*ndim:pdim]
    if pdim < 2*ndim:
        r += [0]*(2*ndim-pdim)
    if wdim < 2*ndim:
        w += [0]*(2*ndim-wdim)
    if sdim < ndim:
        s += [0]*(ndim-sdim)

    # Summarise remaining arguments
    print("Pixel range:", r)
    print("World range:", w)
    print("Skip factor:", s)

    # World coordinates (FITS pixels start at 1)
    cr = [0.0]*ndim*2
    if goworld:
        try:
    #    if goworld:
            # Suppress FITSFixedWarning about PC matrix keyword index labelling
            # wcs = astropy.wcs.WCS(header=hdu[0].header,relax=astropy.wcs.WCSHDR_PC00i00j)
            warnings.simplefilter('ignore', category=AstropyWarning)
            wcs = WCS(header=hdu[0].header)
            # prepare for slicing
            ind1 = slice(0, ndim*2-1, 2)
            ind2 = slice(1, ndim*2  , 2)
            # Requested cutout region in pixels
            cutout = wcs.wcs_world2pix(np.array([w[ind1],w[ind2]]),1)
            # Input cube corners in world coordinates
            corners = wcs.wcs_pix2world(np.array([[1]*ndim,d]),1)
            r[ind1] = cutout[0,:].astype(int)
            r[ind2] = cutout[1,:].astype(int)
            # reset default coordinates
            for i in range(2*ndim):
                if w[i] == 0:
                    r[i] = 0
            # special dispensation for reversed coordinates (returned image not reversed)
            for i in range(ndim):
                if (r[i*2+1]-r[i*2]) < 0.0:
                    r[i*2:i*2+2] = r[i*2:i*2+2][::-1]
                    print("NOTIFICATION: reversing coordinate pair #{}".format(i+1))
            cr[ind1] = corners[0,:]
            cr[ind2] = corners[1,:]
            print("World coordinate range of image:")
            round_coord = ["{:.6g}".format(x) for x in cr]
            print([float(x) for x in round_coord])
        except:
            exit("ERROR: error decoding WCS coordinates - please use pixel coordinates")

    # bail if no output file
    if output == "":
        exit("No output file specified")

    # more parameter checking
    if pdim < 2*nmax:
        na = 2*nmax-pdim
        r = r+[0]*na
    if sdim < nmax:
        nb = nmax-sdim
        s = s+[0]*nb

    # New arrays
    t = [0]*(ndim*2)
    q = [0]*ndim

    # check if pixels lie inside image
    for i in range(ndim):
        if (r[2*i] < 0) or (r[2*i] > d[i]) or (r[2*i+1] < 0) or (r[2*i+1] > d[i]):
            exit("ERROR: selected pixels lie outside image")
        # change (default) skip 0 to 1
        if s[i] == 0:
            s[i] = 1
        # change (default) pixel 0 to min/max
        if r[2*i] == 0:
            r[2*i] = 1
        if r[2*i+1] == 0:
            r[2*i+1] = d[i]
        # new image/cube dimensions
        rdim[i] = int((r[2*i+1]-r[2*i])/s[i]) + 1
        if rdim[i] < 1:
            exit("ERROR: max pixel number is less than min pixel number")
        # reverse order, and decrement start pixel (python first pixel is 0)
        t[2*ndim-2*i-2] = r[2*i]-1
        t[2*ndim-2*i-1] = r[2*i+1]
        # reverse pixel skip order
        q[ndim-i-1] = s[i]

    # update new header (history and CRPIX) parameters
    hdu[0].header['HISTORY'] = "SUBFITS: " + history
    for i in range(ndim):
        # Adjust for offset
        hdu[0].header['CRPIX'+str(i+1)] -= float(r[2*i])-1
        # Adjust for skip
        hdu[0].header['CRPIX'+str(i+1)] = (hdu[0].header['CRPIX'+str(i+1)]-1.0)/float(s[i])+1.0
        hdu[0].header['CDELT'+str(i+1)] = hdu[0].header['CDELT'+str(i+1)]*float(s[i])

    # remove dummy axes (NB this doesn't touch pixel coordinate matrix P)
    if dummy:
        ndim2 = ndim
        # Remove pixel coordinate PC matrix (inconsistent FITS header format makes PC handling tricky)
        del hdu[0].header['PC*_*']
        for i in range(ndim):
            if hdu[0].header['NAXIS'+str(i+1)] == 1:
                print("Removing dummy axis", hdu[0].header['CTYPE'+str(i+1)])
                for j in range(i,ndim2-1):
                    # shift header parameters
                    hdu[0].header['CTYPE'+str(j+1)] = hdu[0].header['CTYPE'+str(j+2)]
                    hdu[0].header['CRVAL'+str(j+1)] = hdu[0].header['CRVAL'+str(j+2)]
                    hdu[0].header['CDELT'+str(j+1)] = hdu[0].header['CDELT'+str(j+2)]
                    hdu[0].header['CRPIX'+str(j+1)] = hdu[0].header['CRPIX'+str(j+2)]
                    hdu[0].header['CUNIT'+str(j+1)] = hdu[0].header['CUNIT'+str(j+2)]
                    rdim[j] = rdim[j+1]
                # remove last header parameters
                del hdu[0].header['CTYPE'+str(ndim2)]
                del hdu[0].header['CRVAL'+str(ndim2)]
                del hdu[0].header['CDELT'+str(ndim2)]
                del hdu[0].header['CRPIX'+str(ndim2)]
                del hdu[0].header['CUNIT'+str(ndim2)]
                del rdim[-1]
                # reduce dimensionality
                ndim2 -= 1
        if ndim2 == 0:
            exit("ERROR: no axes left")
        # (Re-)insert PC values as identity matrix
        for i in range(ndim2):
            for j in range(ndim2):
                if i==j:
                    hdu[0].header['PC_'+str(i+1)+'_'+str(j+1)] = 1.0
                else:
                    hdu[0].header['PC_'+str(i+1)+'_'+str(j+1)] = 0.0

    # output dimensions
    rdim.reverse()
    tdim = tuple(rdim)

    # prepare for slicing
    ind = [slice(None)]*ndim
    for i in range(ndim):
        ind[i] = slice(t[2*i], t[2*i+1], q[i])

    # output FITS file
    try:
        fits.writeto(output, data=hdu[0].data[tuple(ind)].reshape(tdim), header=hdu[0].header)
    except:
        exit("ERROR: unable to write output file")

    # close input
    hdu.close()

    # verify output FITS file
    try:
        hdu2 = fits.open(output, mode='denywrite')
        hdu2.info()
        hdu2.close()
    except:
        exit("ERROR: unable to verify output file")


if __name__ == '__main__':
    argv = sys.argv[1:]
    main(argv)