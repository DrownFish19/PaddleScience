from datetime import datetime
from datetime import timedelta

import cdsapi
from args import EXTERNAL_FORCING_VARS
from args import PRESSURE_LEVELS_ERA5_37
from args import STATIC_VARS
from args import TARGET_ATMOSPHERIC_VARS
from args import TARGET_SURFACE_VARS

all_days = [
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
]
all_times = ["00:00", "06:00", "12:00", "18:00"]
all_levels = PRESSURE_LEVELS_ERA5_37
all_variables = list(
    set(TARGET_SURFACE_VARS)
    .union(set(TARGET_ATMOSPHERIC_VARS))
    .union(set(EXTERNAL_FORCING_VARS))
    .union(set(STATIC_VARS))
    .union(set("total_precipitation"))
)
all_variables.remove("total_precipitation_6hr")

c = cdsapi.Client()


def fetch_one_day(datetime):
    year = "{:02d}".format(datetime.year)
    month = "{:02d}".format(datetime.month)
    day = "{:02d}".format(datetime.day)

    c.retrieve(
        "reanalysis-era5-complete",
        {
            "product_type": "reanalysis",
            "variable": all_variables,
            "pressure_level": all_levels,
            "year": f"{year}",
            "month": f"{month}",
            "day": f"{day}",
            "time": all_times,
            "format": "netcdf",  # Supported format: grib and netcdf. Default: grib
            "area": "global",  # North, West, South, East.          Default: global
            "grid": [
                0.25,
                0.25,
            ],  # Latitude/longitude grid.           Default: 0.25 x 0.25
        },
        f"source-era5_date-{year}-{month}-{day}_res-0.25_levels-37_steps-01.nc",
    )  # Output file. Adapt as you wish.
    print(
        f"source-era5_date-{year}-{month}-{day}_res-0.25_levels-37_steps-01.nc",
    )


def main():
    start_date = datetime(2016, 1, 1)
    end_date = datetime(2016, 1, 3)

    for date in (
        start_date + timedelta(n) for n in range((end_date - start_date).days + 1)
    ):
        print(date.strftime("%Y-%m-%d"))
        fetch_one_day(date)


if __name__ == "__main__":
    main()
