import numpy as np
from astropy.io import fits


def reduce_science_file(science_file_path, flat_files, dark_files, output_reduced_file_path):
    # combine darks:
    darks = [fits.getdata(dark) for dark in dark_files]
    combined_dark = np.nanmedian(darks, axis=0)
    # load and subtract dark from each flat:
    flats = [fits.getdata(flat) - combined_dark for flat in flat_files]
    # combine flats
    combined_flat = np.nanmedian(flats, axis=0)
    # normalize combined flat
    combined_flat /= np.nanmedian(combined_flat)  # make it roughly one
    combined_flat[combined_flat < 1e-8] = 1.0

    # reduce science file.
    reduced_science = (fits.getdata(science_file_path) - combined_dark) / combined_flat
    header = fits.getheader(science_file_path)
    header['REDUC'] = 'bias sub and flat field'

    fits.writeto(output_reduced_file_path, data=reduced_science, header=header, overwrite=True)
