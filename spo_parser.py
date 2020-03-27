import csv
import os
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

OUTPUT_HEADER = [
    'spo_id',
    'lat',
    'long',
    'pollutant',
    'station',
    'network',
    'zone'
]


def parse_dataset_d(folder):
    xtree = et.parse(os.path.join(folder, 'datasetD.xml'))
    xroot = xtree.getroot()
    spo_node_list = xroot.findall('.//aqd:AQD_SamplingPoint', NS)
    content = []
    for spo_node in spo_node_list:
        spo_id = spo_node.attrib['{' + NS['gml'] + '}id']
        lat, long = spo_node.find('.//ef:geometry', NS).find('.//gml:pos', NS).text.split()
        href_attib = '{' + NS['xlink'] + '}href'
        station_node = spo_node.find('.//ef:broader', NS)
        station = station_node.attrib[href_attib].split('/')[1]
        network_node = spo_node.find('.//ef:belongsTo', NS)
        network = network_node.attrib[href_attib].split('/')[1]
        zone_node = spo_node.find('.//aqd:zone', NS)
        zone = zone_node.attrib[href_attib].split('/')[1]
        content.append([spo_id, lat, long, station, network, zone])
    return content


def transform_to_csv(file_location):
    rows = parse_dataset_d(file_location)
    new_filename = os.path.join(file_location, 'spo.csv')
    with open(new_filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_HEADER)
        writer.writerows(rows)


if __name__ == '__main__':
    transform_to_csv('data')
