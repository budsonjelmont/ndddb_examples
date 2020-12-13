import argparse as ap
import xml.etree.ElementTree as ET
import csv

parser = ap.ArgumentParser(description='Parse the CDC\'s XML doc containing the ICD code descriptions to a flat file. These XML are obtained from the CDC FTP here: https://ftp.cdc.gov/pub/health_statistics/nchs/publications/ICD10CM/.')
parser.add_argument('in_xml', metavar='in_xml', type=str, nargs=1, help='Path to the CDC\'s ICD XML.')

args = parser.parse_args()

in_xml = args.in_xml[0]
out_csv = in_xml[:-4] + '_parsed.csv'

csvwriter = csv.writer(open(out_csv, 'w'))

tree = ET.parse(in_xml) # This is the XML
root = tree.getroot()

for diag in root.iter('diag'):           # Loop through every diagnostic tree
   name = diag.find('name').text  # Extract the diag code
   desc = diag.find('desc').text  # Extract the description
   csvwriter.writerow((name,desc))       # Write to .csv
