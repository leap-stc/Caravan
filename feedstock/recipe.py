"""This recipe combines all of the NetCDF files from Caravan V1.6 into a single Zarr store.
Before this recipe, data is downloaded, unzipped and transfered to OSN in *transfer_from_zenodo.sh*"""

import dask
import fsspec
import s3fs
import xarray as xr
import zarr
from distributed import Client
from obstore.store import S3Store
from zarr.storage import ObjectStore

zarr.config.set({'async.concurrency': 128})

client = Client(threads_per_worker=1)

# glob all netcdf files
fs = s3fs.S3FileSystem(anon=True, client_kwargs={'endpoint_url': 'https://nyu1.osn.mghpcc.org/'})
nc_glob_list = fs.glob('leap-pangeo-manual/caravan/Caravan-nc/timeseries/netcdf/**/*.nc')
input_urls = ['https://nyu1.osn.mghpcc.org/' + fil for fil in nc_glob_list]


# func to cache the netcdf files with fsspec, build basin_id dimension based on netcdf filename and drop duplicate time entries.
def cache_and_add_add_basin_id(url: str) -> xr.Dataset:
    fil = fsspec.open_local(
        f'simplecache::{url}', simplecache={'cache_storage': '/tmp/fsspec_cache'}
    )
    ds = xr.open_dataset(fil, engine='netcdf4')
    # create basin_id based on netcdf file suffix
    basin_id = url.split('/')[-1].split('.nc')[0]

    # https://github.com/pydata/xarray/discussions/6297
    ds = ds.expand_dims(dim={'basin_id': [basin_id]})
    all_dims = ds.dims
    indexes = {dim: ~ds.get_index(dim).duplicated(keep='first') for dim in all_dims}
    return ds.isel(indexes)


tasks = [dask.delayed(cache_and_add_add_basin_id)(url) for url in input_urls]

virtual_datasets = dask.compute(tasks)[0]
# concat all datasets
cds = xr.concat(
    virtual_datasets, dim='basin_id', coords='minimal', data_vars='minimal', compat='override'
)  # .chunk({'date':-1, 'basin_id':-1})
# mod attrs
cds.attrs.pop('Timezone')
cds.attrs['DOI'] = 'https://zenodo.org/records/14673536'
cds.attrs['Version'] = '1.6'

# write to OSN pipeline bucket
DATASET_NAME = 'Caravan'

osnstore = S3Store(
    'leap-pangeo-pipeline',
    prefix=f'{DATASET_NAME}/{DATASET_NAME}.zarr',
    aws_endpoint='https://nyu1.osn.mghpcc.org',
    access_key_id='<ADD>',
    secret_access_key='<ADD>',
    client_options={'allow_http': True},
)
zstore = ObjectStore(osnstore)

cds.to_zarr(
    zstore,
    zarr_format=3,
    consolidated=False,
    mode='w',
)
