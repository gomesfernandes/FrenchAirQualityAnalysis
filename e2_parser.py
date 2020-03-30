import csv
from datetime import datetime, timedelta
import pytz
import re
import xml.etree.ElementTree as et

import fetch


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
    d = datetime.fromisoformat(datestring).astimezone(pytz.utc)
    if add_day:
        d = d + timedelta(days=1)
    return d


def parse_file(filename):
    match = re.search(r'-([t|v])\.xml$', filename)
    if not match:
        raise ValueError('Unknown file type')
    file_type = match.group(1)
    xtree = et.parse(filename)
    xroot = xtree.getroot()
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


def transform_to_csv(filename):
    try:
        rows = parse_file(filename)
    except ValueError:
        return
    new_filename = filename.replace('data', 'parsed_data').replace('.xml', '.csv')
    with open(new_filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_HEADER)
        writer.writerows(rows)


if __name__ == '__main__':
    file = 'data/fr-2020-e2-2020-2020-03-30-06-t.xml'
    transform_to_csv(file)
