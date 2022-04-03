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
    "ZBHH": "Hohhot, China",
    "ZBTJ": "Tianjin, China",
    "ZGHA": "Changcha, China",
    "ZHCC": "Zhengzhou, China",
    "ZSHC": "Hangzhou, China",
    "RJAA": "Tokyo, Japan",
    "RJBB": "Osaka, Japan",
    "RKSS": "Seoul, South Korea",
    "UHWW": "Vladivostok, Russia",

    "YSSY": "Sydney, Australia",
    "YMML": "Melbourne, Australia",
    "YBBN": "Brisbane, Australia",
    "YPEA": "Perth, China",
    "NZAA": "Auckland, New Zealand",
    "NZCH": "Christchurch, New Zealand",
    "NZWN": "Wellington, New Zealand",

    "PHX": "Arizona, America",
    "CQT": "Los Angeles, America",
    "NYC": "New York, America",
    "BOS": "Massachusetts, America",
    "ARB": "Michigan, America",
    "MBO": "Mississippi, America",
    "MIA": "Florida, America",
    "SEA": "Washington, America",
    "MDW": "Illinois, America",
    "1M5": "Tennessee, America",
    "CYMX": "Montreal, Canada",
    "CYVR": "Vancouver, Canada",

    "WMSA": "Kuala Lumpur, Malaysia",
    "WMKA": "Alor Setar, Malaysia",
    "WSSS": "Singapore",
    "RPLL": "Manila, Philippines",
    "RPMZ": "Zamboanga, Philippines",
    "VTBD": "Bangkok, Thailand",
    "VVNB": "Hanoi, Vietnam",
    "VDPP": "Phnom-penh, Cambodia",
    "VGEG": "Chittagong, Bangladesh",
    "VQPR": "Thimphu, Bhutan",
    "WIII": "Jakarta, Indonesia",

    "EBBR": "Brussels, Belgium",
    "EBAW": "Antwerp, Belgium",
    "LKPR": "Prague, Czech",
    "LKTB": "Brno, Czech",
    "EKCH": "Copenhagen, Denmark",
    "EKYT": "Aalborg, Denmark",
    "EETN": "Tallinn, Estonia",
    "EFHK": "Helsinli, Finland",
    "EFOU": "Oulu, Finland",
    "LFPO": "Paris, France",
    "LFBD": "Bordeaux, France",
    "EDDB": "Berlin, Germany",
    "EDDM": "Munich, Germany",
    "EGKK": "London, UK",
    "EGPH": "Edinburgh, UK",
    "LGAV": "Athens, Greece",
    "BIRK": "Reykjavik, Iceland",
    "EIDW": "Dublin, Ireland",
    "LIRU": "Rome, Italy",
    "EVRA": "Riga, Latvia",
    "EYVI": "Vilnius, Lithuania",
    "ELLX": "Luxembourg",
    "LUKK": "Chisinau, Moldova",
    "EHAM": "Amsterdam, Netherlands",
    "EHBK": "Maastricht, Netherlands",
    "ENBR": "Bergen, Norway",
    "ENGM": "Oslo, Norway",
    "EPGD": "Gdansk, Poland",
    "EPKK": "Krakow, Poland",
    "EPWA": "Warsaw, Poland",
    "LPPT": "Lisbon, Portugal",
    "UUDD": "Moscow, Russia",
    "ULLI": "St. Petersburg, Russia",
    "ESCM": "Uppsala, Sweden",
    "ESSA": "Stockholm, Sweden",

    "SABE": "Buenos Aires, Argentina",
    "SLET": "Santa Cruz, Bolivia",
    "SBAF": "Rio De Janeiro, Brazil",
    "SCCH": "Chillan, Chile",
    "SKMD": "Medellin, Colombia",
    "MROC": "San Jose, Costa Rica",
    "MUHA": "Havana, Cuba",
    "SECU": "Cuenca, Ecuador",

    "OAJL": "Jalalabad, Afghanistan",
    "OIIE": "Tehran, Iran",
    "ORBI": "Baghdad, Iraq",
    "LLBG": "Tel-aviv, Israel",
    "OJAI": "Amman, Jordan",
    "OKBK": "Kuwait",
    "OLBA": "Beirut, Lebanon",
    "OTBD": "Doha, Qatar",
    "OMDB": "Dubai, UAE",
    "OMAA": "Abu Dhabi, UAE",

    "DAAE": "Bejaja, Algeria",
    "FNLU": "Luanda, Angola",
    "FEFF": "Bangui, Central Africa Republic",
    "FTTA": "Sarh, Chad",
    "FCBB": "Brazzaville, Congo",
    "FZNA": "Goma, Democratic Rep of Congo",
    "HECA": "Cairo, Egypt",
    "FGBT": "Bata, Equatorial Guinea",
    "HAAB": "Addis Ababa, Ethiopia",
    "HKEL": "Eldoret, Kenya",
    "HLLB": "Benghazi, Libya",
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

    logging.info("Done")
