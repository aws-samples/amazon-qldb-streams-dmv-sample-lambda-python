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

import sys, os

sys.path.append(os.path.abspath('../../'))
from qldb_streaming_sample import app
import aws_kinesis_agg
from aws_kinesis_agg.deaggregator import deaggregate_records
from .fixtures import deaggregated_stream_records
from .fixtures import client_error

RETRYABLE_ERRORS = ['ThrottlingException', 'ServiceUnavailable', 'RequestExpired']


def test_handler_for_publishing_sns(mocker, deaggregated_stream_records):
    deaggregated_records = deaggregated_stream_records(revision_version=0)

    # Mock
    mocker.patch('qldb_streaming_sample.app.deaggregate_records', return_value=deaggregated_records)
    mocker.patch.dict(os.environ, {'SNS_ARN': 'an sns arn'})
    mocker.patch('qldb_streaming_sample.app.sns_client.publish', return_value=True)

    # Trigger
    reponse = app.lambda_handler({"Records": ["a dummy record"]}, "")

    # Verify
    app.sns_client.publish.assert_called_with(Message='New User Registered. Name: Nova Lewis', TopicArn='an sns arn')
    assert reponse["statusCode"] == 200


def test_handler_when_records_are_updated(mocker, deaggregated_stream_records):
    deaggregated_records = deaggregated_stream_records(revision_version=1)

    # Mock
    mocker.patch('qldb_streaming_sample.app.deaggregate_records', return_value=deaggregated_records)
    mocker.patch.dict(os.environ, {'SNS_ARN': 'an sns arn'})
    mocker.patch('qldb_streaming_sample.app.sns_client.publish', return_value=True)

    # Trigger
    reponse = app.lambda_handler({"Records": ["a dummy record"]}, "")

    # Verify
    app.sns_client.publish.assert_not_called()
    assert reponse["statusCode"] == 200


def test_handler_when_no_records(mocker, deaggregated_stream_records):
    deaggregated_records = []

    # Mock
    mocker.patch('qldb_streaming_sample.app.deaggregate_records', return_value=deaggregated_records)
    mocker.patch.dict(os.environ, {'SNS_ARN': 'an sns arn'})
    mocker.patch('qldb_streaming_sample.app.sns_client.publish', return_value=True)

    # Trigger
    reponse = app.lambda_handler({"Records": []}, "")

    # Verify
    app.sns_client.publish.assert_not_called()
    assert reponse["statusCode"] == 200


def test_send_sns_notification_retries_for_rertryable_error(mocker, client_error):
    for retryable_error in RETRYABLE_ERRORS:
        # Mock
        error = client_error(retryable_error)
        mocker.patch.dict(os.environ, {'SNS_ARN': 'an sns arn'})
        mocker.patch('qldb_streaming_sample.app.sns_client.publish', side_effect=[error, None])

        # Trigger
        app.send_sns_notification(topic_arn='an sns arn', message='New User Registered. Name: Nova Lewis')

        # verify
        assert app.sns_client.publish.call_count == 2
        app.sns_client.publish.assert_called_with(Message='New User Registered. Name: Nova Lewis',
                                                  TopicArn='an sns arn')


def test_no_retries_for_sns_publish_for_non_retryable_error(mocker, client_error):
    # Mock
    error = client_error("ValidationError")
    mocker.patch.dict(os.environ, {'SNS_ARN': 'an sns arn'})
    mocker.patch('qldb_streaming_sample.app.sns_client.publish', side_effect=[error, None])

    # Trigger
    app.send_sns_notification(topic_arn='an sns arn', message='New User Registered. Name: Nova Lewis')

    # verify
    assert app.sns_client.publish.call_count == 1
    app.sns_client.publish.assert_called_with(Message='New User Registered. Name: Nova Lewis', TopicArn='an sns arn')
