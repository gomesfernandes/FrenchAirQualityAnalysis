"""
A one-time parser for dataset "B" containing information about French zones.
"""
import argparse
import csv
import os
import xml.etree.ElementTree as et

import fetch

DATASET_B_URL = 'https://www.data.gouv.fr/fr/datasets/r/eeebe970-6e2b-47fc-b801-4a38d53fac0d'
OUTPUT_HEADER = [
    'zone_id',
    'zone_code',
    'resident_population',
    'resident_population_year',
    'area_km2',
    'organisation_name',
    'multipolygon',
]


def format_polygon_line(indiviual_points):
    """
    Given a list of coordinates [lat1, long1, lat2, long2, lat3, long3, ...], create a string with
    longitude latitude couples separated by commas, and enclosed in parentheses, ex:
    '(long1 lat1, long2 lat2, long3 lat3, ...)'
    :param indiviual_points: a list of coordinates.
    :return: a formatted polygon line
    """
    formatted_string = '('
    for i in range(0, len(indiviual_points), 2):
        formatted_string += indiviual_points[i + 1] + ' ' + indiviual_points[i] + ','
    formatted_string = formatted_string[:-1]
    formatted_string += ')'
    return formatted_string


def extract_multipolygon(param):
    """
    Parse a gml:MultiSurface element and extract all polygons in WKT format
    :param param:
    :return: a WKT multipolygon
    """
    multipolygon = 'MULTIPOLYGON ('
    polygon_node_list = param.findall('.//gml:Polygon', fetch.NS)
    for polygon_node in polygon_node_list:
        multipolygon += '('

        exterior_points = polygon_node.find('.//gml:exterior', fetch.NS).find('.//gml:posList', fetch.NS).text.split()
        multipolygon += format_polygon_line(exterior_points)

        interior_nodes_list = polygon_node.findall('.//gml:interior', fetch.NS)
        if interior_nodes_list:
            for interior_node in interior_nodes_list:
                multipolygon += ','
                interior_points = interior_node.find('.//gml:posList', fetch.NS).text.split()
                multipolygon += format_polygon_line(interior_points)

        multipolygon += '),'

    multipolygon = multipolygon[:-1]
    multipolygon += ')'
    return multipolygon


def parse_dataset_b(xml_content):
    """
    Extract the fields that interest us
    :param xml_content:
    :return: a list of attributes per zone (cf OUTPUT_HEADER)
    """
    if xml_content is None:
        return []
    xroot = et.fromstring(xml_content)
    zone_node_list = xroot.findall('.//aqd:AQD_Zone', fetch.NS)
    parsed_content = []
    for zone_node in zone_node_list:
        zone_id = zone_node.attrib['{' + fetch.NS['gml'] + '}id']
        zone_code = zone_node.find('.//aqd:zoneCode', fetch.NS).text
        resident_population = zone_node.find('.//aqd:residentPopulation', fetch.NS).text
        resident_population_year_node = zone_node.find('.//aqd:residentPopulationYear', fetch.NS)
        if resident_population_year_node:
            resident_population_year = resident_population_year_node.find('.//gml:timePosition', fetch.NS).text
        else:
            resident_population_year = ''
        area = zone_node.find('.//aqd:area', fetch.NS).text
        organisation_name = zone_node.find('.//base2:organisationName', fetch.NS).find('.//gco:CharacterString',
                                                                                       fetch.NS).text
        multipolygon = extract_multipolygon(zone_node.find('.//gml:MultiSurface', fetch.NS))
        parsed_content.append([
            zone_id,
            zone_code,
            resident_population,
            resident_population_year,
            area,
            organisation_name,
            multipolygon
        ])
    return parsed_content


def transform_to_csv(outdir):
    xml_content = fetch.fetch_file_content(DATASET_B_URL)
    rows = parse_dataset_b(xml_content)
    new_filename = os.path.join(outdir, 'zone.csv')
    with open(new_filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_HEADER)
        writer.writerows(rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--outdir', help='Output directory (default = `data`)', default='data')
    args = parser.parse_args()
    outdir = args.outdir
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    transform_to_csv(args.outdir)
