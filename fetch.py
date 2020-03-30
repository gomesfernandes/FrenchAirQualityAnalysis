"""
This module makes calls to the data.gouv.fr API to list the available resources on the LCSQA Air Quality page and to
download individual datasets.
Note that you must set the 'DATA_FR_API_KEY' environment variable to the API key.
"""
import datetime
import os
import requests
import pytz

BASE_URI = 'https://www.data.gouv.fr/api/1'
AIR_QUALITY_URI = '/datasets/5b98b648634f415309d52a50'
HEADERS = {'X-API-KEY': os.environ.get('DATA_FR_API_KEY')}
YESTERDAY = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=1)
NS = {
    'gml': 'http://www.opengis.net/gml/3.2',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'aqd': 'http://dd.eionet.europa.eu/schemaset/id2011850eu-1.0',
    'base': 'http://inspire.ec.europa.eu/schemas/base/3.3',
    'base2': 'http://inspire.ec.europa.eu/schemas/base2/1.0',
    'ef': 'http://inspire.ec.europa.eu/schemas/ef/3.0',
    'ompr': 'http://inspire.ec.europa.eu/schemas/ompr/2.0',
    'xlink': 'http://www.w3.org/1999/xlink',
    'sam': 'http://www.opengis.net/sampling/2.0',
    'sams': 'http://www.opengis.net/samplingSpatial/2.0',
    'gmd': 'http://www.isotc211.org/2005/gmd',
    'gco': 'http://www.isotc211.org/2005/gco',
    'om': 'http://www.opengis.net/om/2.0',
    'swe': 'http://www.opengis.net/swe/2.0',
    'am': 'http://inspire.ec.europa.eu/schemas/am/3.0',
    'ad': 'urn:x-inspire:specification:gmlas:Addresses:3.0',
    'gn': 'urn:x-inspire:specification:gmlas:GeographicalNames:3.0'
}


def fetch_all_resources():
    """
    Fetch all resources currently listed in the LCSQA Air Quality page on data.gouv.fr.
    Note that this includes documentation files and others.
    :return: a json object with all resources descriptions, ordered by most recent to oldest
            returns None if the status code was not 200
    """
    response = requests.get(BASE_URI + AIR_QUALITY_URI, headers=HEADERS)
    if response.status_code != 200:
        return None
    return sorted(response.json()["resources"], reverse=True, key=lambda k: k['created_at'])


def fetch_file_content(resource_url):
    """
    Fetch the XML content of a resource via its URL
    :param resource_url
    :return: the text content of the file, None if the status code was not 200
    """
    response = requests.get(resource_url, headers={'accept': 'application/xml'})
    if response.status_code != 200:
        return None
    return response.text


def filter_e2_files_by_date(resources, date):
    """
    Filter the list of resources to include only the E2 files (type 'main') for a specific day.
    :param resources: the list of all resources returned by the API call
    :param date:
    :return: a list of resources created on the given day
    """
    return str([r for r in resources if (r['type'] == 'main' and same_date(r['created_at'], date))])


def same_date(dt_str, dt):
    """
    :param dt_str:
    :param dt:
    :return: True if dt_str has the same date as dt, False otherwise
    """
    try:
        transformed_datetime = datetime.datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        transformed_datetime = datetime.datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')
    return transformed_datetime.date() == dt.date()


if __name__ == '__main__':
    resources = fetch_all_resources()
    print(resources)
    #print(filter_e2_files_by_date(resources, YESTERDAY))
