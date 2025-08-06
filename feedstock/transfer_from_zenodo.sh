# download first using uv tool run zenodo_get https://doi.org/10.5281/zenodo.15529786

# extract netcdf and stream output to osn
tar -zxf Caravan-nc.tar.gz --wildcards "*.nc" --to-command="rclone rcat le
apmanual:leap-pangeo-manual/caravan/\$TAR_FILENAME"
