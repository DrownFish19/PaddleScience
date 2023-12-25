import datetime
import multiprocessing

import cdsapi

INPUT_SURFACE_VARS = [
    "divergence",
    "fraction_of_cloud_cover",
    "geopotential",
    "ozone_mass_mixing_ratio",
    "potential_vorticity",
    "relative_humidity",
    "specific_cloud_ice_water_content",
    "specific_cloud_liquid_water_content",
    "specific_humidity",
    "specific_rain_water_content",
    "specific_snow_water_content",
    "temperature",
    "u_component_of_wind",
    "v_component_of_wind",
    "vertical_velocity",
    "vorticity",
]
INPUT_SURFACE_SINGLE_LEVEL_VARS = [
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "2m_dewpoint_temperature",
    "2m_temperature",
    "geopotential",
    "land_sea_mask",
    "toa_incident_solar_radiation",
    "total_precipitation",
]
PRESSURE_LEVEL = [
    "1",
    "2",
    "3",
    "5",
    "7",
    "10",
    "20",
    "30",
    "50",
    "70",
    "100",
    "125",
    "150",
    "175",
    "200",
    "225",
    "250",
    "300",
    "350",
    "400",
    "450",
    "500",
    "550",
    "600",
    "650",
    "700",
    "750",
    "775",
    "800",
    "825",
    "850",
    "875",
    "900",
    "925",
    "950",
    "975",
    "1000",
]
MONTHES = [
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
]
DAYS = [
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
HOURS = [
    "00:00",
    "01:00",
    "02:00",
    "03:00",
    "04:00",
    "05:00",
    "06:00",
    "07:00",
    "08:00",
    "09:00",
    "10:00",
    "11:00",
    "12:00",
    "13:00",
    "14:00",
    "15:00",
    "16:00",
    "17:00",
    "18:00",
    "19:00",
    "20:00",
    "21:00",
    "22:00",
    "23:00",
]
SELECTED_HOURS = HOURS = [
    "00:00",
    "06:00",
    "12:00",
    "18:00",
]

c = cdsapi.Client()


def fetch_one_day_surface_vars(datetime):
    year = "{:02d}".format(datetime.year)
    month = "{:02d}".format(datetime.month)
    day = "{:02d}".format(datetime.day)
    file_name = (
        f"surface-source-era5_date-{year}-{month}-{day}_res-0.25_levels-37_steps-01.nc"
    )
    request_dict = {
        "product_type": "reanalysis",
        "variable": INPUT_SURFACE_VARS,
        "pressure_level": PRESSURE_LEVEL,
        "year": year,
        "month": month,
        "day": day,
        "time": SELECTED_HOURS,
        "format": "netcdf",
        "grid": [0.25, 0.25],
    }
    c.retrieve(
        "reanalysis-era5-pressure-levels",
        request_dict,
        file_name,
    )
    return f"{file_name} downloaded"


def fetch_one_day_surface_single_level_vars(datetime):
    year = "{:02d}".format(datetime.year)
    month = "{:02d}".format(datetime.month)
    day = "{:02d}".format(datetime.day)
    file_name = f"source-era5_date-{year}-{month}-{day}_res-0.25_levels-01_steps-01.nc"
    request_dict = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "variable": INPUT_SURFACE_SINGLE_LEVEL_VARS,
        "year": year,
        "month": MONTHES,
        "day": DAYS,
        "time": HOURS,
        "grid": [0.25, 0.25],
    }
    c.retrieve(
        "reanalysis-era5-single-levels",
        request_dict,
        file_name,
    )
    return f"{file_name} downloaded"


def main():
    start_date = datetime.datetime(2017, 1, 1, 0, 0, 0)
    end_date = datetime.datetime(2020, 1, 1, 0, 0, 0)

    timedate_days = [
        start_date + datetime.timedelta(n)
        for n in range((end_date - start_date).days + 1)
    ]
    timedate_years = [
        datetime.datetime(y, 1, 1) for y in range(start_date.year, end_date.year + 1)
    ]

    # single process
    # for date in timedate_days:
    #     fetch_one_day_surface_vars(date)
    # for date in timedate_years:
    #     fetch_one_day_surface_single_level_vars(date)

    with multiprocessing.Pool(processes=4) as pool:
        results_days = pool.imap(fetch_one_day_surface_vars, timedate_days)
        for result in results_days:
            print(result)
    with multiprocessing.Pool(processes=4) as pool:
        results_years = pool.imap(
            fetch_one_day_surface_single_level_vars, timedate_years
        )
        for result in results_years:
            print(result)


if __name__ == "__main__":
    main()
