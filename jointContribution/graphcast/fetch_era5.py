import datetime

import cdsapi
import requests
import threading


def download_chunk(url, start, end, filename):
    headers = {"Range": f"bytes={start}-{end}"}
    r = requests.get(url, headers=headers, stream=True)
    with open(filename, "r+b") as fp:
        fp.seek(start)
        fp.write(r.content)


def download_file(url, filename, num_threads=4):
    r = requests.head(url)
    try:
        file_size = int(r.headers["content-length"])
    except:
        print("无法获取文件大小")
        return

    part = file_size // num_threads
    fp = open(filename, "wb")
    fp.truncate(file_size)
    fp.close()

    for i in range(num_threads):
        start = part * i
        end = start + part
        if i == num_threads - 1:
            end = file_size
        threading.Thread(
            target=download_chunk, args=(url, start, end, filename)
        ).start()


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
SELECTED_HOURS = [
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

c = cdsapi.Client()


def fetch_one_day_surface_vars(datetime):
    year = "{:02d}".format(datetime.year)
    month = "{:02d}".format(datetime.month)
    day = "{:02d}".format(datetime.day)

    urls = []
    for hour in SELECTED_HOURS:
        file_name = f"surface-source-era5_date-{year}-{month}-{day}-{hour[:2]}_res-0.25_levels-37_steps-01.nc"
        print(file_name)
        request_dict = {
            "product_type": "reanalysis",
            "variable": INPUT_SURFACE_VARS,
            "pressure_level": PRESSURE_LEVEL,
            "year": year,
            "month": month,
            "day": day,
            "time": hour,
            "format": "netcdf",
            "grid": [0.25, 0.25],
        }
        res = c.retrieve(
            "reanalysis-era5-pressure-levels",
            request_dict,
            # file_name,
        )
        download_file(url=res.location, filename=file_name, num_threads=8)
        urls.append(str(res.location) + ", " + str(file_name))
    return "\n".join(urls)


def fetch_one_day_surface_single_level_vars(datetime):
    year = "{:02d}".format(datetime.year)
    month = "{:02d}".format(datetime.month)
    day = "{:02d}".format(datetime.day)

    urls = []
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
    res = c.retrieve(
        "reanalysis-era5-single-levels",
        request_dict,
        # file_name,
    )
    download_file(url=res.location, filename=file_name, num_threads=8)
    urls.append(str(res.location) + ", " + str(file_name))
    return "\n".join(urls)


def main():
    start_date = datetime.datetime(2018, 1, 1)
    end_date = datetime.datetime(2020, 6, 30)

    result_file_name = "-".join(
        [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")]
    )
    result_file = open(f"data-{result_file_name}.csv", "w")

    timedate_days = [
        start_date + datetime.timedelta(n)
        for n in range((end_date - start_date).days + 1)
    ]
    timedate_years = [
        datetime.datetime(y, 1, 1) for y in range(start_date.year, end_date.year + 1)
    ]
    # for single thread
    for timedate_day in timedate_days:
        urls = fetch_one_day_surface_vars(timedate_day)
        print(urls, flush=True)
        result_file.write(urls + "\n")
        result_file.flush()

    for timedate_year in timedate_years:
        urls = fetch_one_day_surface_single_level_vars(timedate_year)
        print(urls, flush=True)
        result_file.write(urls + "\n")
        result_file.flush()

    # for multiprocessing
    # import multiprocessing
    # index = 0
    # with multiprocessing.Pool(processes=1) as pool:
    #     results_days = pool.imap(fetch_one_day_surface_vars, timedate_days)
    #     for url, filename in results_days:
    #         print(url, filename)
    #         result_file.write(str(url) + ", " + str(filename) + "\n")
    #         result_file.flush()
    #         index += 1

    #         if index % 30 == 0:
    #             result_file.write("\n")
    #             result_file.flush()

    # with multiprocessing.Pool(processes=1) as pool:
    #     results_years = pool.imap(
    #         fetch_one_day_surface_single_level_vars, timedate_years
    #     )
    #     for url, filename in results_years:
    #         print(url, filename)
    #         result_file.write(str(url) + ", " + str(filename) + "\n")
    #         result_file.flush()


if __name__ == "__main__":
    main()
