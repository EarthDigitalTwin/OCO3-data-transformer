try:
    from importlib.metadata import version as _version
except ImportError:
    from importlib_metadata import version as _version

try:
    __version__ = _version('oco3_sam_zarr')
except Exception:
    __version__ = '999'
