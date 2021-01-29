CREATE TABLE db.era5_data (
    ts timestamp NULL,
    lat float8 NULL,
    lon float8 NULL,
    eastward_wind_at_100_metres float4 NULL,
    eastward_wind_at_10_metres float4 NULL,
    northward_wind_at_100_metres float4 NULL,
    northward_wind_at_10_metres float4 NULL,
    air_pressure_at_mean_sea_level float4 NULL,
    air_temperature_at_2_metres float4 NULL,
    dew_point_temperature_at_2_metres float4 NULL,
    snow_density float4 NULL,
    wind_speed_10m float4 NULL,
    wind_direction_10m float4 NULL,
    wind_speed_100m float4 NULL,
    wind_direction_100m float4 NULL,
    CONSTRAINT pk_era5_data PRIMARY KEY ("ts", "lat", "lon")
) PARTITION BY RANGE ("ts");

CREATE TABLE era5_data_y1949_1987 PARTITION OF era5_data
    FOR VALUES FROM ('1949-01-01 00:00:00') TO ('1987-12-31 23:59:59');
CREATE TABLE era5_data_y1988_y1995 PARTITION OF era5_data
    FOR VALUES FROM ('1988-01-01 00:00:00') TO ('1995-12-31 23:59:59');
CREATE TABLE era5_data_y1996_2003 PARTITION OF era5_data
    FOR VALUES FROM ('1996-01-01 00:00:00') TO ('2003-12-31 23:59:59');
CREATE TABLE era5_data_y2004_2011 PARTITION OF era5_data
    FOR VALUES FROM ('2004-01-01 00:00:00') TO ('2011-12-31 23:59:59');
CREATE TABLE era5_data_y2012_2019 PARTITION OF era5_data
    FOR VALUES FROM ('2012-01-01 00:00:00') TO ('2019-12-31 23:59:59');
CREATE TABLE era5_data_y2020_2029 PARTITION OF era5_data
    FOR VALUES FROM ('2020-01-01 00:00:00') TO ('2029-12-31 23:59:59');