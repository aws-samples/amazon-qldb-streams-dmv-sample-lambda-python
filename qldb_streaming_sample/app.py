# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import amazon.ion.simpleion as ion
import base64
from aws_kinesis_agg.deaggregator import deaggregate_records
import boto3
import os
from botocore.exceptions import ClientError

# SNS client
sns_client = boto3.client('sns')

REVISION_DETAILS_RECORD_TYPE = "REVISION_DETAILS"
PERSON_TABLENAME = "Person"
VEHICLE_REGISTRATION_TABLENAME = "VehicleRegistration"

RETRYABLE_ERRORS = ['ThrottlingException', 'ServiceUnavailable', 'RequestExpired']


def lambda_handler(event, context):
    """
    Triggered for a batch of kinesis records.
    Parses QLDB Journal streams and  sends an SNS notification for Person and Vehicle Registration Events.
    """

    sns_topic_arn = os.environ['SNS_ARN']
    raw_kinesis_records = event['Records']

    # Deaggregate all records in one call
    records = deaggregate_records(raw_kinesis_records)

    # Iterate through deaggregated records
    for record in records:

        # Kinesis data in Python Lambdas is base64 encoded
        payload = base64.b64decode(record['kinesis']['data'])
        # payload is the actual ion binary record published by QLDB to the stream
        ion_record = ion.loads(payload)
        print("Ion reocord: ", (ion.dumps(ion_record, binary=False)))

        if (("recordType" in ion_record) and (ion_record["recordType"] == REVISION_DETAILS_RECORD_TYPE)):

            revision_data, revision_metadata = get_data_metdata_from_revision_record(ion_record)
            table_info = get_table_info_from_revision_record(ion_record)

            if (revision_metadata["version"] == 0):  # a new record inserted
                if (table_info and table_info["tableName"] == PERSON_TABLENAME and person_data_has_required_fields(
                        revision_data)):
                    send_sns_notification(sns_topic_arn,
                                          'New User Registered. Name: {first_name} {last_name}'
                                          .format(first_name=revision_data["FirstName"],
                                           last_name=revision_data["LastName"]))

                elif (table_info and table_info[
                    "tableName"] == VEHICLE_REGISTRATION_TABLENAME and vehicle_registration_data_has_required_fields(
                    revision_data)):
                    send_sns_notification(sns_topic_arn, 'New Vehicle Registered. '
                                          'VIN: {vin}, LicensePlateNumber: {license_plate_number}'
                                          .format(vin=revision_data["VIN"],
                                           license_plate_number=revision_data["LicensePlateNumber"]))
            else:
                print("No Action")

    return {
        'statusCode': 200
    }


def get_data_metdata_from_revision_record(revision_record):
    """
    Retrieves the data block from revision Revision Record

    Parameters:
       topic_arn (string): The topic you want to publish to.
       message (string): The message you want to send.
    """

    revision_data = None
    revision_metadata = None

    if ("payload" in revision_record) and ("revision" in revision_record["payload"]):
        if ("data" in revision_record["payload"]["revision"]):
            revision_data = revision_record["payload"]["revision"]["data"]
        if ("metadata" in revision_record["payload"]["revision"]):
            revision_metadata = revision_record["payload"]["revision"]["metadata"]

    return [revision_data, revision_metadata]


def get_table_info_from_revision_record(revision_record):
    """
    Retrieves the table information block from revision Revision Record
    Table information contains the table name and table id
    """

    if (("payload" in revision_record) and "tableInfo" in revision_record["payload"]):
        return revision_record["payload"]["tableInfo"]


def send_sns_notification(topic_arn, message):
    """
    Sends SNS notification to topic_arn.
    Retries once for Retryable Errors.

    Parameters:
       topic_arn (string): The topic you want to publish to.
       message (string): The message you want to send.
    """

    for _ in range(0, 2):
        try:
            sns_client.publish(
                TopicArn=topic_arn,
                Message=message
            )
            print("SNS published to topic : " + topic_arn + " with message : " + message)
            break
        except ClientError as e:  # https://docs.aws.amazon.com/sns/latest/api/CommonErrors.html
            if e.response['Error']['Code'] in RETRYABLE_ERRORS:
                print("Caught retryable error : ", e)
                pass
            else:
                # Non retryable error
                print("Caught non retryable error : ", e)
                break


def person_data_has_required_fields(revision_data):
    return (("FirstName" in revision_data) and ("LastName" in revision_data))


def vehicle_registration_data_has_required_fields(revision_data):
    return (("VIN" in revision_data) and ("LicensePlateNumber" in revision_data))
