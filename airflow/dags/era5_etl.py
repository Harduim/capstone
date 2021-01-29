import logging as log
import os
from datetime import datetime, timedelta
from typing import List, Tuple
from dateutil.relativedelta import relativedelta
from glob import glob
from time import perf_counter
from pendulum.constants import YEARS_PER_CENTURY
from operator import lt, gt, eq

import numpy as np
import xarray as xr

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import chain
from airflow.operators import DataQualityOperator
from botocore.exceptions import ClientError
from pendulum.pendulum import Pendulum
from collections import namedtuple


S3_BUCKET = "era5-pds"
DATA_PATH = "{year}/{month}/data/{var}.nc"
WEATHERDB_CONN_ID = "weatherdb"
AWS_CONN_ID = "aws"

TEMPDATA = os.path.join(os.environ["AIRFLOW_HOME"], "tempdata")
if not os.path.isdir(TEMPDATA):
    os.mkdir(TEMPDATA)


ERA5_VARS = [
    "snow_density",
    "air_pressure_at_mean_sea_level",
    "dew_point_temperature_at_2_metres",
    "eastward_wind_at_10_metres",
    "eastward_wind_at_100_metres",
    "northward_wind_at_10_metres",
    "northward_wind_at_100_metres",
    "air_temperature_at_2_metres",
]

DAYS_ON_TABLE_QUERY = """DROP TABLE IF EXISTS TTBL;
    SELECT date_part('DAY', TIME0) AS dd
    INTO TEMP TTBL
    FROM staging.{var}
    GROUP BY date_part('YEAR', TIME0)
            ,date_part('MONTH', TIME0)
            ,date_part('DAY', TIME0);

    SELECT CASE WHEN count(dd) BETWEEN 28 AND 31 THEN 0 ELSE 1 END
    FROM TTBL;"""


class Ti:
    execution_date = Pendulum.now()


class FilterCoords:
    """South america. """

    # lat_min, lat_max = -50, 10
    # lon_min, lon_max = 100, 150
    lat_min, lat_max = -5, 10
    lon_min, lon_max = 10, 15


def uv_to_wswd(u, v):
    ws = np.sqrt(u ** 2 + v ** 2)
    wd = 180 + (180 / np.pi) * np.arctan2(u, v)
    return ws, wd


def wind_speed_direction():
    hook = PostgresHook(WEATHERDB_CONN_ID)
    wdf = hook.get_pandas_df(
        """SELECT u100.time0,
            u100.lat,
            u100.lon,
            eastward_wind_at_100_metres AS u_wind_100m,
            eastward_wind_at_10_metres AS u_wind_10m,
            northward_wind_at_100_metres AS v_wind_100m,
            northward_wind_at_10_metres AS v_wind_10m
        FROM staging.eastward_wind_at_100_metres AS u100
        INNER JOIN staging.eastward_wind_at_10_metres AS u10 ON u100.time0 = u10.time0 AND u100.lat = u10.lat AND u100.lon = u10.lon
        INNER JOIN staging.northward_wind_at_100_metres AS v100 ON u100.time0 = v100.time0 AND u100.lat = v100.lat AND u100.lon = v100.lon
        INNER JOIN staging.northward_wind_at_10_metres AS v10 ON u100.time0 = v10.time0 AND u100.lat = v10.lat AND u100.lon = v10.lon;"""
    )

    wdf["wind_speed_100m"], wdf["wind_direction_100m"] = uv_to_wswd(
        wdf["u_wind_100m"], wdf["v_wind_100m"]
    )
    wdf["wind_speed_10m"], wdf["wind_direction_10m"] = uv_to_wswd(
        wdf["u_wind_10m"], wdf["v_wind_10m"]
    )
    cols = [
        "time0",
        "lat",
        "lon",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_speed_100m",
        "wind_direction_100m",
    ]
    wdf = wdf.loc[:, cols]

    t_start = perf_counter()

    csv_path = os.path.join(TEMPDATA, "wind_speed_direction.csv")
    wdf.to_csv(csv_path, index=False, header=False, sep="\t")
    hook.bulk_load("staging.wind_speed_direction", csv_path)

    log.info(f"Bulk insert took {perf_counter()-t_start:.2f}")

    os.remove(csv_path)


def check_new(**context):
    _date = context.get("ti", Ti).execution_date - relativedelta(months=1)
    expected_file = f"{_date.year}/{_date.month}/main.nc"

    s3 = S3Hook(aws_conn_id="aws")
    bucket = s3.get_bucket(S3_BUCKET)
    try:
        bucket.Object(expected_file).load()
    except ClientError:
        log.error(f"s3://{S3_BUCKET}/{expected_file} not found")
        raise

    log.info(f"\n{expected_file} found\n")


def reset_tempdata():
    for fl in glob(os.path.join(TEMPDATA, "*.*")):
        log.info(f"Deleting {fl}")
        # os.remove(fl)


def download_datafile(var_name: str, **context):
    _date = context.get("ti", Ti).execution_date - relativedelta(months=1)

    remote_file = f"{_date.year}/{_date.month}/data/{var_name}.nc"
    local_file = os.path.join(TEMPDATA, f"{var_name}.nc")

    s3 = S3Hook(aws_conn_id="aws")
    bucket = s3.get_bucket(S3_BUCKET)
    log.info(f"\nDownloading s3://{S3_BUCKET}/{remote_file} to {local_file}\n")
    # bucket.download_file(remote_file, local_file)


def stage_dataset(var):
    hook = PostgresHook(WEATHERDB_CONN_ID)
    dataset = xr.open_dataset(os.path.join(TEMPDATA, f"{var}.nc"))

    for time0 in dataset.time0:
        ts = time0.values
        log.info(f"Staging: {var}=>[{ts}]\n")

        t_start = perf_counter()
        df = dataset.sel(time0=ts).to_dataframe()
        log.info(f"Read took {perf_counter()-t_start:.2f}\n")

        t_start = perf_counter()
        df.reset_index(inplace=True)

        df_query = f"lat >= {FilterCoords.lat_min} and lat <= {FilterCoords.lat_max}"
        df_query = f"{df_query} and lon >= {FilterCoords.lon_min} and lon <= {FilterCoords.lon_max}"

        df = df.query(df_query).copy()
        log.info(f"Filter took {perf_counter()-t_start:.2f}\n")

        t_start = perf_counter()
        csv_path = os.path.join(TEMPDATA, f"{var}.csv")
        df.to_csv(csv_path, index=False, header=False, sep="\t")
        hook.bulk_load(f"staging.{var}", csv_path)

        log.info(f"Bulk insert took {perf_counter()-t_start:.2f}")

        os.remove(csv_path)


def test_result(math_operator: callable, expected_result: int):
    return lambda r, _: math_operator(r[0], expected_result)


with DAG(
    "era5_etl",
    description="""Load and transform data from S3 to Postgres""",
    schedule_interval="@monthly",
    default_args={
        "owner": "Arthur Harduim",
        "depends_on_past": False,
        "start_date": datetime(2020, 11, 1),
        "email": ["arthur@rioenergy.com.br"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "catchup": True,
    },
    max_active_runs=1,
) as dag:
    clean_tempdata = PythonOperator(
        task_id="clean_tempdata",
        python_callable=reset_tempdata,
    )
    task_check_new = PythonOperator(
        task_id="Check_new_files_on_s3",
        python_callable=check_new,
        provide_context=True,
    )
    join_datasets = PostgresOperator(
        task_id="join_datasets",
        autocommit=True,
        postgres_conn_id=WEATHERDB_CONN_ID,
        sql="""INSERT INTO db.era5_data
            SELECT u100.time0 AS ts,
                u100.lat,
                u100.lon,
                eastward_wind_at_100_metres,
                eastward_wind_at_10_metres,
                northward_wind_at_100_metres,
                northward_wind_at_10_metres,
                air_pressure_at_mean_sea_level,
                air_temperature_at_2_metres,
                dew_point_temperature_at_2_metres,
                snow_density,
                wind_speed_10m,
                wind_direction_10m,
                wind_speed_100m,
                wind_direction_100m
            FROM staging.eastward_wind_at_100_metres AS u100
            INNER JOIN staging.eastward_wind_at_10_metres AS u10 ON u100.time0 = u10.time0 AND
                u100.lat = u10.lat AND u100.lon = u10.lon
            INNER JOIN staging.northward_wind_at_100_metres AS v100 ON u100.time0 = v100.time0 AND
                u100.lat = v100.lat AND u100.lon = v100.lon
            INNER JOIN staging.northward_wind_at_10_metres AS v10 ON u100.time0 = v10.time0 AND
                u100.lat = v10.lat AND u100.lon = v10.lon
            INNER JOIN staging.air_pressure_at_mean_sea_level AS apl ON u100.time0 = apl.time0 AND
                u100.lat = apl.lat AND u100.lon = apl.lon
            INNER JOIN staging.air_temperature_at_2_metres AS atm ON u100.time0 = atm.time0 AND
                u100.lat = atm.lat AND u100.lon = atm.lon
            INNER JOIN staging.dew_point_temperature_at_2_metres AS dt ON u100.time0 = dt.time0 AND
                u100.lat = dt.lat AND u100.lon = dt.lon
            INNER JOIN staging.snow_density AS sn ON u100.time0 = sn.time0 AND
                u100.lat = sn.lat AND u100.lon = sn.lon
            INNER JOIN staging.wind_speed_direction AS wsd ON u100.time0 = wsd.time0 AND
                u100.lat = wsd.lat AND u100.lon = wsd.lon
            ON CONFLICT DO NOTHING;""",
    )
    clean_staging_wind_speed_table = PostgresOperator(
        task_id="clean_staging_wind_speed_table",
        autocommit=True,
        postgres_conn_id=WEATHERDB_CONN_ID,
        sql="""DROP TABLE IF EXISTS staging.wind_speed_direction;
                CREATE TABLE staging.wind_speed_direction (
                    time0 timestamp NULL,
                    lat float8 NULL,
                    lon float8 NULL,
                    wind_speed_10m float4 NULL,
                    wind_direction_10m float4 NULL,
                    wind_speed_100m float4 NULL,
                    wind_direction_100m float4 NULL
                );
                CREATE INDEX wind_speed_direction_time0_idx ON
                staging.wind_speed_direction USING btree (time0, lon, lat);""",
    )
    calc_windspeed_direction = PythonOperator(
        task_id="calc_windspeed_direction",
        python_callable=wind_speed_direction,
    )
    data_quality_coords = DataQualityOperator(
        task_id="data_quality_coords",
        conn_id=WEATHERDB_CONN_ID,
        test_cases=(
            ("SELECT MAX(lat) FROM db.era5_data", test_result(eq, FilterCoords.lat_max)),
            ("SELECT MIN(lat) FROM db.era5_data", test_result(eq, FilterCoords.lat_min)),
            ("SELECT MAX(lon) FROM db.era5_data", test_result(eq, FilterCoords.lon_max)),
            ("SELECT MIN(lon) FROM db.era5_data", test_result(eq, FilterCoords.lon_min)),
        ),
    )
    data_quality_time = DataQualityOperator(
        task_id=f"data_quality_time",
        conn_id=WEATHERDB_CONN_ID,
        test_cases=(("SELECT COUNT(time0) FROM db.era5_data", test_result(gt, 10000)),),
    )
    chain(join_datasets, [data_quality_coords, data_quality_time])
    for var in ERA5_VARS:
        clean_staging_tables = PostgresOperator(
            task_id=f"clean_staging_{var}",
            autocommit=True,
            postgres_conn_id=WEATHERDB_CONN_ID,
            sql=f"""DROP TABLE IF EXISTS staging.{var};
                CREATE TABLE staging.{var} (
                    lat float8 NULL,
                    lon float8 NULL,
                    time0 timestamp NULL,
                    {var} float4 NULL
                );
                CREATE INDEX {var}_time0_idx ON staging.{var} USING btree (time0, lon, lat);""",
        )
        download_var_task = PythonOperator(
            task_id=f"download_{var}",
            python_callable=download_datafile,
            op_args=[var],
            provide_context=True,
        )
        stage_task = PythonOperator(
            task_id=f"stage_{var}",
            python_callable=stage_dataset,
            op_args=[var],
        )

        stage_quality_gate_time = DataQualityOperator(
            task_id=f"time_quality_gate_{var}",
            conn_id=WEATHERDB_CONN_ID,
            test_cases=(
                (f"SELECT COUNT(time0) FROM staging.{var}", test_result(gt, 10000)),
                (DAYS_ON_TABLE_QUERY.format(var=var), test_result(eq, 0)),
            ),
        )
        stage_quality_gate_coords = DataQualityOperator(
            task_id=f"coords_quality_gate_{var}",
            conn_id=WEATHERDB_CONN_ID,
            test_cases=(
                (f"SELECT MAX(lat) FROM staging.{var}", test_result(eq, FilterCoords.lat_max)),
                (f"SELECT MIN(lat) FROM staging.{var}", test_result(eq, FilterCoords.lat_min)),
                (f"SELECT MAX(lon) FROM staging.{var}", test_result(eq, FilterCoords.lon_max)),
                (f"SELECT MIN(lon) FROM staging.{var}", test_result(eq, FilterCoords.lon_min)),
            ),
        )
        chain(
            [clean_tempdata, task_check_new],
            clean_staging_tables,
            download_var_task,
            stage_task,
            [stage_quality_gate_coords, stage_quality_gate_time],
            clean_staging_wind_speed_table,
            calc_windspeed_direction,
            join_datasets,
        )
