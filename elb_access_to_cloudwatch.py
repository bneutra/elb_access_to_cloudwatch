#!/usr/bin/env python3
# Functions to write elb/alb csv files to cloudwatch

import boto3
import botocore
import time
import datetime
import csv
import json
import sys
import gzip
import io
from operator import itemgetter

def csv_to_cw_list(file_data, lb_type='elb'):
    """elb/alb log file (string) converted from csv to a list of cw json events."""
    elb_fields = [
        'timestamp',
        'elb',
        'client:port',
        'backend:port',
        'request_processing_time',
        'backend_processing_time',
        'response_processing_time',
        'elb_status_code',
        'backend_status_code',
        'received_bytes',
        'sent_bytes',
        'request',
        'user_agent',
        'ssl_cipher',
        'ssl_protocol'
    ]

    alb_fields = [
        "target_group_arn",
        "trace_id",
        "domain_name",
        "chosen_cert_arn",
        "matched_rule_priority",
        "request_creation_time",
        "actions_executed",
        "redirect_url",
        "error_reason",
        "target:port_list",
        "target_status_code_list",
        "classification",
        "classification_reason"
    ]

    float_fields = [
        'request_processing_time',
        'backend_processing_time',
        'response_processing_time'
    ]
    int_fields = [
        'received_bytes',
        'sent_bytes'
    ]
    if lb_type == 'alb':
        elb_fields.insert(0, 'type') # elb's don't have this field
        elb_fields += alb_fields # add alb fields
    csvreader = csv.DictReader(file_data, tuple(elb_fields), delimiter=' ')
    cw_events = []
    for row in csvreader:
        if not row.get('timestamp'): continue
        for field in float_fields:
            row[field] = float(row.get(field) if row.get(field) else 0)
        for field in int_fields:
            row[field] = int(row.get(field) if row.get(field) else 0)
        cw_events.append(get_cw_json(row))
    return cw_events

def get_cw_json(row):
    # use the event timestamp to mark the cloudwatch timestamp (hmm, seems to drop the milliseconds)
    dt = datetime.datetime.strptime(row['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    #epoch = int(dt.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
    epoch = 1605741904000 +  random.randint(0,10000)
    return {
        'timestamp': epoch,
        'message': json.dumps(row)
    }

def write_cloudwatch_logs(events, log_group, log_stream):
    """write list of cw json events in bulk to cloudwatch logs."""
    logs = boto3.client('logs', region_name='us-east-1')
    logs.create_log_stream(logGroupName=log_group, logStreamName=log_stream)

    response = logs.put_log_events(
        logGroupName=log_group,
        logStreamName=log_stream,
        logEvents=events[0:1000]
    )
    print(response)

# local file
#def main():
#    fl = sys.argv[1]
#    file_data = open(fl, newline = '').readlines()

# s3
def s3_to_cwatch(log_group, bucket, path):
# args LOG_GROUP BUCKET BUCKET_PATH

    s3 = boto3.client('s3') 
    response = s3.get_object(Bucket=bucket, Key=path) 
    content = response['Body'].read()
    file_data = gzip.decompress(content).decode().split('\n')
    lb_type = 'alb'
    if file_data[0].startswith("20"): lb_type='elb'
    cw_events = csv_to_cw_list(file_data, lb_type=lb_type)
    sorted_events = sorted(cw_events, key=itemgetter('timestamp'))
    i = 0
    while True:
        # cw logs has 1MB limit, 1000 alb events should be well under that
        if i+1000 > len(sorted_events)-1:
            print(f'writing events {i} to {len(sorted_events)-1}')
            write_cloudwatch_logs(sorted_events[i:], log_group, f'{path.split("/")[-1]}.{i}')
            break
        else:
            print(f'writing events {i} to {i+999}')
            write_cloudwatch_logs(sorted_events[i:i+999], log_group, f'{path.split("/")[-1]}.{i}')
        i += 1000

if __name__ == '__main__':
    log_group = sys.argv[1]
    bucket = sys.argv[2]
    path = sys.argv[3]
    sys.exit(s3_to_cwatch(log_group, bucket, path))