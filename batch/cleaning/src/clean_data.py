#!/usr/bin/python
import re
import json
import csv
import io
import os
from zipfile import ZipFile
import boto3
from io import StringIO
import logging

SELECTED_FIELDS = [
    'Year', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
    'CRSDepTime', 'DepTime', 'DepDelay', 'DepDelayMinutes', 'CRSArrTime', 'ArrTime', 'ArrDelay', 'ArrDelayMinutes',
    'Cancelled'
]

s3client = boto3.client('s3')

logger = logging.getLogger()
if 'DEBUG' in os.environ and os.environ['DEBUG'] == 'true':
    logger.setLevel(logging.DEBUG)
    logger.debug('Debug mode enabled.')
else:
    logger.setLevel(logging.INFO)


def write_new_csvfiles(dest_bucket, dest_prefix, data):
    """
    Will write the processed data as an object in an S3 bucket.
    """
    results = {
        'written_lines': 0,
        'written_files': []
    }
    for year, year_data in data['data'].items():
        for month, month_data in year_data.items():
            for day, day_data in month_data.items():
                output_file = f"{dest_prefix}/{year:04d}/{month:02d}/{year:04d}_{month:02d}_{day:02d}.csv"
                output = StringIO()
                writer = csv.DictWriter(output, fieldnames=day_data[0].keys())
                writer.writeheader()
                writer.writerows(day_data)
                results['written_lines'] += len(day_data)

                logging.debug(f'Putting CSV file {output_file} onto S3.')
                s3client.put_object(
                    Bucket=dest_bucket,
                    Key=output_file,
                    Body=output.getvalue(),
                    ContentType='text/csv'
                )
                output.close()
                results['written_files'].append({
                    'bucketname': dest_bucket,
                    'key': output_file
                })
                logging.info(f'Data file {output_file} written to S3 (lines: {len(day_data)}).')
    return results


def process_csvfile(csvfile, csvlines):
    """
    Parses contents of a csvfile, selects a number of fields and adds some aggregated fields. Returns a dict
    containing the files to be written and uploaded into S3. Content of dict is grouped by year, month, and day.
    """
    logging.info(f"Processing CSV file {csvfile}.")
    output_data = {}

    csvreader = csv.DictReader(csvlines)

    nr_lines = 0
    skipped_lines = 0
    logging.debug(f'Fieldnames: {json.dumps(sorted(csvreader.fieldnames))}')
    for line in csvreader:
        try:
            output_line = {i: line[i] for i in SELECTED_FIELDS}
            year = int(line['Year'])
            month = int(line['Month'])
            day = int(line['DayofMonth'])

            if year not in output_data:
                output_data[year] = {}
            if month not in output_data[year]:
                output_data[year][month] = {}
            if day not in output_data[year][month]:
                output_data[year][month][day] = []
            output_data[year][month][day].append(output_line)
            nr_lines += 1
        except IndexError as e:
            logging.error(f"Error processing line {line} in {csvfile}. ({e})")
    logging.info(f"Processed {nr_lines} lines from {csvfile}.")
    return {
        'csvfile': csvfile,
        'processed_nr_lines': nr_lines,
        'skipped_nr_lines': skipped_lines,
        'data': output_data
    }


def download_and_extract_zipfile(source_bucket, source_key):
    """
    Downloads a zipfile from S3 and returns a list of csv file(s) contained in the zipfile.
    Each item in the list is a dict containing:
        - filename
        - content of the csv file (as a list of lines)
        - number of lines in the csv file.
    """
    logging.info(f'Downloading object {source_bucket}/{source_key} from S3.')
    image = s3client.get_object(
        Bucket=source_bucket,
        Key=source_key
    )

    csvfiles = []
    logging.info(f'Opening and retrieving CSV files from {source_key}.')
    tf = io.BytesIO(image["Body"].read())
    tf.seek(0)
    zipfile = ZipFile(tf, mode='r')
    for file in zipfile.namelist():
        if re.search('.csv$', file):
            logging.debug(f'CSV file {file} found in object.')
            lines = zipfile.open(file).readlines()
            item = {
                'filename': file,
                'content': lines,
                'nr_lines': len(lines) - 1
            }
            logging.info(f'CSV file {file} read into memory.')
            csvfiles.append(item)
    tf.close()
    return csvfiles


def handle_zipfile(event, context):
    """
    Event is expected as a dictionary:
    {
        'src-bucketname': <s3-bucket>,
        'key': <location-of-zipfile>,
        'dst-bucketname': <s3-bucket>,
        'dst-key-prefix': <prefix>
    }
    """
    global validation_ok
    logging.info(f'Handler invoked for {event["src-bucketname"]}/{event["key"]}')
    csvfiles = download_and_extract_zipfile(
        event['src-bucketname'],
        event['key']
    )
    for csvfile in csvfiles:

        logging.info(f'Processing data from {csvfile["filename"]}')
        processed_data = process_csvfile(csvfile['filename'], csvfile['content'])

        logging.info(f'Writing processed data from {csvfile["filename"]}')
        results = write_new_csvfiles(event['dst-bucketname'], event['dst-key-prefix'], processed_data)

        logging.info(f'Validating written data from {csvfile["filename"]}')
        validation_ok = 1
        if csvfile['nr_lines'] != (processed_data['processed_nr_lines'] + processed_data['skipped_nr_lines']):
            logging.error(f'Not all lines from {csvfile["filename"]} were processed. (read: {csvfile["nr_lines"]} vs processed: {processed_data["processed_nr_lines"]}).')
            validation_ok = 0
        else:
            logging.info(f'All lines read from {csvfile["filename"]} were processed.')
        if csvfile['nr_lines'] != (results['written_lines'] + processed_data['skipped_nr_lines']):
            logging.error(f'Not all lines from {csvfile["filename"]} were written to S3. (read: {csvfile["nr_lines"]} vs written: {results["written_lines"]}).')
            validation_ok = 0
        else:
            logging.info(f'All lines read from {csvfile["filename"]} were written to S3 after processing.')

        if validation_ok == 1:
            logging.info(f'--- Processing of file {csvfile["filename"]} completed successfully.')
        else:
            logging.error(f'+++ Error occurred while processing file {csvfile["filename"]}.')
    return {
        'status': 'OK' if validation_ok == 1 else 'ERROR'
    }
