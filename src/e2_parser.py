import argparse
import csv
from datetime import datetime
from datetime import timedelta
import os
import re
import tempfile
import xml.etree.ElementTree as et

from dateutil.parser import isoparse
from google.cloud import storage
import pytz

from src import fetch

YESTERDAY = datetime.now(pytz.utc) - timedelta(days=1)
OUTPUT_HEADER = [
    'file_type',
    'network_code',
    'station_code',
    'pollutant_code',
    'start_date',
    'end_date',
    'verification',
    'validity',
    'pollutant_quantity'
]


def parse_date(datestring):
    """
    Parse a date string in ISO format, handling cases where time is indicated as 'T24:00:00'.
    Returns the corresponding datetime.
    """
    add_day = False
    if re.search(r'T24:00:00', datestring):
        datestring = datestring.replace('T24:00:00', 'T00:00:00')
        add_day = True
    d = isoparse(datestring).astimezone(pytz.utc)
    if add_day:
        d = d + timedelta(days=1)
    return d


def parse_content(xml_content, file_type):
    """Given the XML content of an E2 file, extract the attributes we want (including file_type)
    :param xml_content: the XML content of the file
    :param file_type: 't' for the hourly unverified measurements, 'v' for measurements in verified files
    :return: a list with attributes for each measurement
    """
    xroot = et.fromstring(xml_content)
    observations = xroot.findall('.//om:OM_Observation', fetch.NS)
    content = []
    for observation_node in observations:
        obs_attrib = observation_node.attrib['{' + fetch.NS['gml'] + '}id'].split('_')
        network_code = obs_attrib[-4]
        station_code = obs_attrib[-3]
        pollutant_code = obs_attrib[-2]
        num_observations = list(observation_node.find('.//swe:Count', fetch.NS))[0].text
        if int(num_observations) > 0:
            measurements = observation_node.find('.//swe:values', fetch.NS).text.rstrip('@@').split('@@')
            for single_measurement in measurements:
                values = single_measurement.split(',')
                start_date = parse_date(values[0])
                end_date = parse_date(values[1])
                verification = values[2]
                validity = values[3]
                pollutant_quantity = values[4]
                content.append([
                    file_type,
                    network_code,
                    station_code,
                    pollutant_code,
                    start_date,
                    end_date,
                    verification,
                    validity,
                    pollutant_quantity
                ])
    return content


def format_resource_to_csv(resource):
    """
    Reads and parses the XML content of a single resource.
    Raises a ValueError if the name of the dataset does not match the usual format, that is ending in -t or -v,
    which indicated if the measurements were verified or not.
    :param resource: a dict with information about one resource
    :return: a tuple, the first item being the list of rows returned by parse_content, the second one being
            a suggestion for a new filename
    """
    match = re.search(r'-([t|v])\.xml$', resource['url'])
    if not match:
        raise ValueError('Unknown file type')
    file_type = match.group(1)
    xml_content = fetch.fetch_file_content(resource['url'])
    rows = parse_content(xml_content, file_type)
    new_filename = resource['url'].split('/')[-1].replace('.xml', '.csv')
    return rows, new_filename


def get_resources_for_date(date):
    """
    Retrieve E2 files published on the given date. Note that the files may contain measurements made on
    the day before.
    :param date:
    :return: a list of resources corresponding to the E2 datasets posted on the given date
    """
    return fetch.filter_e2_files_by_date(fetch.fetch_all_resources(), date)


def write_to_file(rows, filename, directory, with_header=False):
    """
    Create or overwrite a CSV file in a given directory
    :param rows: a list of rows to write
    :param filename: the new filename
    :param directory: the destination directory
    :param with_header: True if the OUTPUT_HEADER should be the first line in the file
    """
    with open(os.path.join(directory, filename), 'w', newline='') as f:
        writer = csv.writer(f)
        if with_header:
            writer.writerow(OUTPUT_HEADER)
        writer.writerows(rows)


def write_to_bucket(rows, filename, bucket, with_header=False):
    """
    Create or overwrite a CSV file in a given GCS bucket
    :param rows: a list of rows to write
    :param filename: the new filename
    :param bucket: the destination bucket
    :param with_header: True if the OUTPUT_HEADER should be the first line in the file
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(filename)

    with tempfile.TemporaryFile('r+', newline='') as tmp_f:
        writer = csv.writer(tmp_f)
        if with_header:
            writer.writerow(OUTPUT_HEADER)
        writer.writerows(rows)
        blob.upload_from_file(tmp_f, rewind=True, content_type='text/csv')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--outdir', help='Output directory (default = `parsed_data`)', default='parsed_data')
    parser.add_argument('--date',
                        help='A date to filter the results (default is yesterday)',
                        type=lambda s: isoparse(s),
                        default=YESTERDAY,
                        required=False)
    parser.add_argument('--no-header', action="store_false", default=True)
    args = parser.parse_args()

    outdir = args.outdir
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    resources_list = get_resources_for_date(args.date)
    for resource in resources_list:
        rows, new_filename = format_resource_to_csv(resource)
        write_to_file(rows, new_filename, outdir, with_header=args.no_header)
        # write_to_bucket(rows, new_filename, 'airqualitylcsqa', with_header=False)
