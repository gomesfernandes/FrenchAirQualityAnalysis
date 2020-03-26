import csv
from datetime import datetime, timedelta
import pytz
import re
import xml.etree.ElementTree as et

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


def parse_date(datestring):
    '''
    Parse a date string in ISO format, handling cases where time is indicated as 'T24:00:00'.
    Returns the corresponding datetime.
    '''
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
    observations = xroot.findall('.//om:OM_Observation', NS)
    content = []
    for observation_node in observations:
        obs_attrib = observation_node.attrib['{' + NS['gml'] + '}id'].split('_')
        network_code = obs_attrib[-4]
        station_code = obs_attrib[-3]
        pollutant_code = obs_attrib[-2]
        num_observations = list(observation_node.find('.//swe:Count', NS))[0].text
        if int(num_observations) > 0:
            measurements = observation_node.find('.//swe:values', NS).text.rstrip('@@').split('@@')
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
    except ValueError as err:
        return
    header = [
        'file_type',
        'network code',
        'station_code',
        'pollutant_code',
        'start_date',
        'end_date',
        'verification',
        'validity',
        'pollutant_quantity'
    ]
    new_filename = filename.replace('data', 'parsed_data').replace('.xml', '.csv')
    with open(new_filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


if __name__ == '__main__':
    file = 'data/fr-2020-e2-2020-2020-03-26-07-t.xml'
    transform_to_csv(file)
