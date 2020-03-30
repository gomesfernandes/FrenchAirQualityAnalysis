"""
This module makes calls to the data.gouv.fr API to list the available resources on the LCSQA Air Quality page and to
download individual datasets.
Note that you must set the 'DATA_FR_API_KEY' environment variable to the API key.
"""
import os
import requests

BASE_URI = 'https://www.data.gouv.fr/api/1'
AIR_QUALITY_URI = '/datasets/5b98b648634f415309d52a50'
HEADERS = {'X-API-KEY': os.environ.get('DATA_FR_API_KEY')}


def fetch_all_resources():
    """
    Fetch all resources currently listed in the LCSQA Air Quality page on data.gouv.fr
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


if __name__ == '__main__':
    resources = fetch_all_resources()
    print(resources)
