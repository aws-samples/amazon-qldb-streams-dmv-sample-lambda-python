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
import pytest
from botocore.exceptions import ClientError


def person_revision_details_ion_record(revision_version):
    person_revision_details_ion = """{
      qldbStreamArn: "arn:aws:qldb:us-east-1:466800022684:stream/vehicle-registration/17CR7ArZ1AMHgHOeOk6G9C",
      recordType: "REVISION_DETAILS",
      payload: {
        tableInfo: {
          tableName: "Person",
          tableId: "1SUXCa3wwV0GD7kV78RbSg"
        },
        revision: {
          blockAddress: {
            strandId: "HbD9IggL584EPHmfwjmVz0",
            sequenceNo: 3
          },
          hash: {{vJFOcsNRM14gsIBSEnwPhMVgRAWf/4EUW5gPYbtmDv0=}},
          data: {
            FirstName: "Nova",
            LastName: "Lewis",
            DOB: 1963-08-19T,
            GovId: "LEWISR261LL",
            GovIdType: "Driver License",
            Address: "1719 University Street, Seattle, WA, 98109"
          },
          metadata: {
            id: "D35qd3e2prnJYmtKW6kok1",
            version: """ + str(revision_version) + """,
            txTime: 2019-12-11T07:20:51.245Z,
            txId: "0007KbqoyqAIch6XRbQ4iA"
          }
        }
      }
    }"""

    return person_revision_details_ion


def person_block_summary_ion_record():
    PERSON_BLOCK_SUMMARY_ION = """{
      qldbStreamArn: "arn:aws:qldb:us-east-1:466800022684:stream/vehicle-registration/17CR7ArZ1AMHgHOeOk6G9C",
      recordType: "BLOCK_SUMMARY",
      payload: {
        blockAddress: {
          strandId: "HbD9IggL584EPHmfwjmVz0",
          sequenceNo: 3
        },
        transactionId: "0007KbqoyqAIch6XRbQ4iA",
        blockTimestamp: 2019-12-11T07:20:51.261Z,
        blockHash: {{lu425dAWsmvzxuNHTbn4ID4mLo0bWKkjLP2Uel4wrPQ=}},
        entriesHash: {{RNGQGcOKCGLCo5S+hs1eboanNrocIzRiqzzq1s99G/Q=}},
        previousBlockHash: {{pV28aszpqJH9LOO9oMsACDmXfdzdEW7HYxzuQVIjSDU=}},
        entriesHashList: [
          {{LmcGQjLlfScQQxbzaoglEpXpeN9bp7I/QUk690ncEpk=}},
          {{vJFOcsNRM14gsIBSEnwPhMVgRAWf/4EUW5gPYbtmDv0=}},
          {{KXJrG8t/KePERHasyztlv4kZPol4Q2buhWmy7iJrsiY=}},
          {{Lz3XWBwtWyBA/Lhj+UoLhbajPQ8Mk9N4j0HJlrm2OTg=}}
        ],
        transactionInfo: {
          statements: [
            {
              statement: "INSERT INTO Person <<\\n{\\n    'FirstName' : 'Testing new 600',\\n    'LastName' : 'Lewis',\\n    'DOB' : `1963-08-19T`,\\n    'GovId' : 'LEWISR261LL',\\n    'GovIdType' : 'Driver License',\\n    'Address' : '1719 University Street, Seattle, WA, 98109'\\n}\\n>>",
              startTime: 2019-12-11T07:20:51.223Z,
              statementDigest: {{t5wbRW+wIi/X0n3iPhFJtbt2qpzzgWkOXIFC4xJHp4o=}}
            }
          ],
          documents: {
            D35qd3e2prnJYmtKW6kok1: {
              tableName: "Person",
              tableId: "1SUXCa3wwV0GD7kV78RbSg",
              statements: [
                0
              ]
            }
          }
        },
        revisionSummaries: [
          {
            hash: {{vJFOcsNRM14gsIBSEnwPhMVgRAWf/4EUW5gPYbtmDv0=}},
            documentId: "D35qd3e2prnJYmtKW6kok1"
          }
        ]
      }
    }"""

    return PERSON_BLOCK_SUMMARY_ION


@pytest.fixture
def deaggregated_stream_records():
    def deaggregated_records(revision_version=0):
        return [{
            'kinesis': {
                'kinesisSchemaVersion': '1.0',
                'sequenceNumber': '49602202223557391954992952781210718954161044299731435522',
                'approximateArrivalTimestamp': 1576048851.397,
                'explicitHashKey': None,
                'partitionKey': 'HrBDrkUho57HQir4ootmLy',
                'subSequenceNumber': 0,
                'aggregated': True,
                'data': base64.b64encode(ion.dumps(ion.loads(person_block_summary_ion_record()))).decode("utf-8")
            },
            'eventSource': 'aws:kinesis',
            'eventVersion': '1.0',
            'eventID': 'shardId-000000000000:49602202223557391954992952781210718954161044299731435522',
            'eventName': 'aws:kinesis:record',
            'invokeIdentityArn': 'arn:aws:iam::466800022684:role/dmv-streaming-python-lamb-RegistrationNotifierLamb-8UAMSTQEIHVP',
            'awsRegion': 'us-east-1',
            'eventSourceARN': 'arn:aws:kinesis:us-east-1:466800022684:stream/RegistrationNotificationStreamKinesis'
        }, {
            'kinesis': {
                'kinesisSchemaVersion': '1.0',
                'sequenceNumber': '49602202223557391954992952781210718954161044299731435522',
                'approximateArrivalTimestamp': 1576048851.397,
                'explicitHashKey': None,
                'partitionKey': '5kelIAWsbRF7U2OSKyj6Wm',
                'subSequenceNumber': 1,
                'aggregated': True,
                'data': base64.b64encode(
                    ion.dumps(ion.loads(person_revision_details_ion_record(revision_version)))).decode("utf-8")
            },
            'eventSource': 'aws:kinesis',
            'eventVersion': '1.0',
            'eventID': 'shardId-000000000000:49602202223557391954992952781210718954161044299731435522',
            'eventName': 'aws:kinesis:record',
            'invokeIdentityArn': 'arn:aws:iam::466800022684:role/dmv-streaming-python-lamb-RegistrationNotifierLamb-8UAMSTQEIHVP',
            'awsRegion': 'us-east-1',
            'eventSourceARN': 'arn:aws:kinesis:us-east-1:466800022684:stream/RegistrationNotificationStreamKinesis'
        }]

    return deaggregated_records


@pytest.fixture
def client_error():
    def client_error_helper(error_code):
        response = {"Error": {"Code": error_code}}
        client_error = ClientError(error_response=response, operation_name="TestOperation");

        return client_error

    return client_error_helper
