from datetime import datetime
from datetime import timedelta
from itertools import product

import cdsapi
from args import EXTERNAL_FORCING_VARS
from args import PRESSURE_LEVELS_ERA5_37
from args import STATIC_VARS
from args import TARGET_ATMOSPHERIC_VARS
from args import TARGET_SURFACE_VARS

all_levels = PRESSURE_LEVELS_ERA5_37
all_variables = list(
    set(TARGET_SURFACE_VARS)
    .union(set(TARGET_ATMOSPHERIC_VARS))
    .union(set(EXTERNAL_FORCING_VARS))
    .union(set(STATIC_VARS))
    .union(set(["total_precipitation"]))
)
all_variables.remove("total_precipitation_6hr")

c = cdsapi.Client()


def fetch_one_day(datetime, var, level):
    year = "{:02d}".format(datetime.year)
    month = "{:02d}".format(datetime.month)
    day = "{:02d}".format(datetime.day)
    hour = "{:02d}".format(datetime.hour)

    request_dict = {
        "product_type": "reanalysis",
        "variable": var,
        "pressure_level": level,
        "year": f"{year}",
        "month": f"{month}",
        "day": f"{day}",
        "time": f"{hour}:00",
        "format": "netcdf",  # Supported format: grib and netcdf. Default: grib
        "area": "global",  # North, West, South, East.          Default: global
        "grid": [
            0.25,
            0.25,
        ],  # Latitude/longitude grid.           Default: 0.25 x 0.25
    }

    c.retrieve(
        name="reanalysis-era5-complete",
        request=request_dict,
        target=f"source-era5_date-{year}-{month}-{day}_vat-{var}_res-0.25_levels-{level}_steps-01.nc",
    )


def main():
    start_date = datetime(2016, 1, 1, 0, 0, 0)
    end_date = datetime(2016, 1, 3, 0, 0, 0)

    hours = int((end_date - start_date).total_seconds() / 3600) + 1
    timedates = [start_date + timedelta(hours=n) for n in range(hours)]
    for date, var, level in product(timedates, all_variables, all_levels):
        print(date.strftime("%Y-%m-%d %H:%M:%S"), var, level)
        fetch_one_day(date, var, level)


if __name__ == "__main__":
    main()
