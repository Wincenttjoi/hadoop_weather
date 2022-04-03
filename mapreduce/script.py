import concurrent.futures
import logging
import os
import sys
import threading

import requests

stations = {
    "ZBAA": "Beijing, China",
    "ZUUU": "Chengdu, China",
    "ZGGG": "Guangzhou, China",
    "YSSY": "Sydney, Australia",
    "YMML": "Melbourne, Australia",
    "PHX": "Arizona, America",
    "CQT": "Los Angeles, America",
    "NYC": "New York, America",
    "BOS": "Massachusetts, America",
    "ARB": "Michigan, America",
    # "MBO": "Mississippi, America",  # deleted, too much missing data
    "MIA": "Florida, America",
    "SEA": "Washington, America",
    "MDW": "Illinois, America",
    "WMSA": "Kuala Lumpur, Malaysia",
    "WMKA": "Alor Setar, Malaysia",
    "WSSS": "Singapore",
}


class Jobs:
    def __init__(self, count):
        self.total = count
        self.__finished = 0
        self.__mutex = threading.Lock()

    def finish(self):
        self.__mutex.acquire()
        self.__finished += 1
        finished = self.__finished
        self.__mutex.release()
        return finished


def get_url(station: str) -> str:
    """
    Get the downloading url of weather data at station.

    Data includes temperature, humidity, wind speed and wind direction from Jan 2012 (inclusive) to Jan 2022 (exclusive).
    :param station: station code
    :return: data downloading url
    """

    s = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?" \
        "station={station:s}&data=tmpc&data=relh&data=drct&data=sped&" \
        "year1=2012&month1=1&day1=1&year2=2022&month2=1&day2=1&tz=Etc%2FUTC&" \
        "format=onlycomma&latlon=no&elev=yes&missing=M&trace=T&direct=no&report_type=1&report_type=2"
    return s.format(station=station)


def download_data(station: str, jobs: Jobs):
    url = get_url(station)
    filepath = "{directory:s}/{station:s}.csv".format(directory=dir, station=station)

    logging.info("Started downloading: " + station)
    resp = requests.get(url, allow_redirects=True)
    with open(filepath, 'wb') as file:
        file.write(resp.content)
    filesize = os.path.getsize(filepath) / 1024
    logging.info("Finished downloading {finished}/{total}: {station}".format(
        finished=jobs.finish(), total=jobs.total, station=station) + "(size = %.2f MB)" % filesize)


if __name__ == "__main__":
    # set up logging
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    # check argv
    try:
        dir = sys.argv[1]
        if dir[-1] == '/':
            dir = dir[:-1]
    except IndexError:
        logging.error('Missing argument "directory". Use: python script.py directory')
        sys.exit(1)

    # set up job counter
    jobs = Jobs(len(stations))

    # execute parallel downloading
    with concurrent.futures.ThreadPoolExecutor() as pool:
        for station in stations.keys():
            if not os.path.exists("{directory:s}/{station:s}.csv".format(directory=dir, station=station)):
                pool.submit(download_data, station, jobs)
            else:
                jobs.finish()
                logging.info("Skipping " + station)

    logging.info("Data downloading is done")
