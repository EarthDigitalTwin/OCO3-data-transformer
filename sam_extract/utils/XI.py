# Copyright 2025 California Institute of Technology (Caltech)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import gc
import logging
import os
import sys
import threading
from tempfile import TemporaryDirectory
from typing import Tuple

import numpy as np

# TODO: It may be worthwhile to update the naming in and around this module to something more clear than XI (which is
#  the parameter name in the interpolation methods this is used for...

logger = logging.getLogger(__name__)

XI_LOCK = threading.Lock()
XI: Tuple[np.ndarray, np.ndarray, np.ndarray] | None = None
XI_DIR: TemporaryDirectory | None = None
F_XI: str | None = None
F_LAT: str | None = None
F_LON: str | None = None


def _unload_np_to_disk(path: str, a: np.ndarray, compress=False):
    """
    Map a numpy array to disk, so it can be unloaded from memory.

    Usage:
    a = _unload_np_to_disk(f, a)

    At high grid resolutions, this pipeline can consume excessive memory. This can be used to pull np arrays
    from memory when they're not in use / when they don't need to be in memory.

    :param path: Path to where the array is to be stored
    :param a: The array
    :return: The array but as a memory-mapped array
    """

    if compress:
        np.savez_compressed(path, a=a)
        ret = np.load(path, mmap_mode='r')['a']
    else:
        np.save(path, a)
        ret = np.load(path, mmap_mode='r')

    logger.debug(f'Unloaded np ndarray of shape {a.shape} and type {a.dtype} to file at {path} '
                 f'{sys.getsizeof(a):,} -> {sys.getsizeof(ret):,}')

    return ret


def get_xi(cfg) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    A lot of space is wasted by generating the xi parameter for scipy's griddata individually for each
    worker thread, here we try to make a single, global instance of the XI param since it will be identical
    for each thread.

    :param cfg: Runtime config dictionary
    :return: Tuple: xi, longitude coordinate array, latitude coordinate array
    """
    global XI, XI_DIR, F_XI, F_LAT, F_LON

    with XI_LOCK:
        if XI is not None:
            return XI

        logger.info('Building coordinate meshes for global interpolations')

        if XI_DIR is None:
            XI_DIR = TemporaryDirectory(prefix='oco-sam-extract-', suffix='-grid-coords', ignore_cleanup_errors=True)

        lon_grid, lat_grid = np.mgrid[-180:180:complex(0, cfg.grid['longitude']),
                                      -90:90:complex(0, cfg.grid['latitude'])].astype(np.dtype('float32'))

        # scipy seems to do this internally if we give xi as a tuple, negating any memory savings
        # lets do this in advance so the process isn't repeated

        logger.debug(f'Lat/lon grids: {lon_grid.shape}, {lat_grid.shape}')

        xi = (lon_grid, lat_grid)

        p = list(np.broadcast_arrays(*xi))
        points = np.empty(p[0].shape + (len(xi),), dtype=np.dtype('float32'))

        logger.debug(f'Points nd array shape: {points.shape}')

        for j, item in enumerate(p):
            points[..., j] = item

        temp_dir = XI_DIR.name

        F_XI = os.path.join(temp_dir, 'xi.npy')
        F_LAT = os.path.join(temp_dir, 'lat.npy')
        F_LON = os.path.join(temp_dir, 'lon.npy')

        logger.debug('Mapping shared coordinate arrays to disk')

        lats = lat_grid[0]
        lons = lon_grid.transpose()[0]

        xi_mm  = _unload_np_to_disk(F_XI,  points)
        lat_mm = _unload_np_to_disk(F_LAT, lats)
        lon_mm = _unload_np_to_disk(F_LON, lons)

        XI = (xi_mm, lon_mm, lat_mm)

        del lon_grid, lat_grid, lats, lons, xi, p, points

        collected = gc.collect()
        logger.debug(f'GC collected {collected:,} objects to free post-xi gen')
        logger.debug(f'Xi nd array: {XI[0].shape}')

        return XI


def get_f_xi():
    return F_XI


def cleanup_xi():
    if XI_DIR is not None:
        logger.info(f'Cleaning up shared coordinate mesh data in {XI_DIR.name}')
        XI_DIR.cleanup()
