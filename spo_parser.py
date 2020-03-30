"""
A one-time parser for the dataset "D", which contains information about the Sample Points (SPOs) in the E2 datasets.
For each SPO, we retrieve the following information:
    * spo_id : the SPOs identifier
    * lat : latitude
    * long : longitude
    * pollutant : the type of pollutant measured by this SPO
    * station : the ID of the station this SPO belongs to
    * network : the ID of the network this SPO is part of
    * zone : the ID zone of the station this SPO is part of
"""
import argparse
import csv
import os
import xml.etree.ElementTree as et

import fetch

DATASET_D_URL = 'https://www.data.gouv.fr/fr/datasets/r/64807938-5d57-4947-a4b5-e9100e28df5d'
OUTPUT_HEADER = [
    'spo_id',
    'lat',
    'long',
    'pollutant',
    'station',
    'network',
    'zone'
]


def read_dataset_d():
    """
    :return: the XML content of dataset D
    """
    xml_content = fetch.fetch_file_content(DATASET_D_URL)
    return xml_content


def parse_dataset_d(xml_content):
    """
    Extract the fields that interest us (SPOs)
    :param xml_content:
    :return: a list of attributes per SPO (cf OUTPUT_HEADER)
    """
    if xml_content is None:
        return []
    xroot = et.fromstring(xml_content)
    spo_node_list = xroot.findall('.//aqd:AQD_SamplingPoint', fetch.NS)
    parsed_content = []
    for spo_node in spo_node_list:
        spo_id = spo_node.attrib['{' + fetch.NS['gml'] + '}id']
        lat, long = spo_node.find('.//ef:geometry', fetch.NS).find('.//gml:pos', fetch.NS).text.split()
        href_attib = '{' + fetch.NS['xlink'] + '}href'
        station_node = spo_node.find('.//ef:broader', fetch.NS)
        station = station_node.attrib[href_attib].split('/')[1]
        network_node = spo_node.find('.//ef:belongsTo', fetch.NS)
        network = network_node.attrib[href_attib].split('/')[1]
        zone_node = spo_node.find('.//aqd:zone', fetch.NS)
        zone = zone_node.attrib[href_attib].split('/')[1]
        pollutant = spo_id.split('_')[-1]
        parsed_content.append([spo_id, lat, long, pollutant, station, network, zone])
    return parsed_content


def transform_to_csv(outdir):
    xml_content = read_dataset_d()
    rows = parse_dataset_d(xml_content)
    new_filename = os.path.join(outdir, 'spo.csv')
    with open(new_filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_HEADER)
        writer.writerows(rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--outdir', help='Output directory (default = `data`)', default='data')
    parser.parse_args()
    transform_to_csv('data')
