import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from publish_trades import make_event
from setup_aws import firehose_request, ident, names


class SetupAwsTests(unittest.TestCase):
    def test_names_are_scoped_to_prefix_account_region(self):
        n = names('fraud-fintech', '123456789012', 'us-west-2')
        self.assertEqual(n['bucket'], 'fraud-fintech-123456789012-us-west-2')
        self.assertEqual(n['storage_int'], 'FRAUD_FINTECH_S3_INT')
        self.assertEqual(n['firehose_stream'], 'fraud-fintech-trades')

    def test_rejects_unsafe_identifiers(self):
        for bad in ['DB; DROP', 'a-b', '1abc', '']:
            with self.assertRaises(ValueError):
                ident(bad)

    def test_firehose_request_matches_aws_schema(self):
        import botocore.session
        from botocore.validate import validate_parameters
        n = names('fraud-fintech', '123456789012', 'us-west-2')
        req = firehose_request(n, n['bucket'], 'arn:aws:iam::123456789012:role/fraud-fintech-firehose-s3')
        model = botocore.session.get_session().get_service_model('firehose')
        validate_parameters(req, model.operation_model('CreateDeliveryStream').input_shape)
        dest = req['ExtendedS3DestinationConfiguration']
        self.assertEqual(dest['Prefix'], 'trades/')
        self.assertFalse(dest['ErrorOutputPrefix'].startswith('trades/'))

    def test_trade_event_matches_pipe_columns(self):
        import random
        event = make_event(random.Random(7))
        self.assertEqual(set(event), {'account_id', 'event_ts', 'notional_usd', 'self_match_pct', 'status', 'sent_ms'})
        self.assertRegex(event['account_id'], r'^ACC-00[0-3]\d$')
        self.assertIn(event['status'], ('ALERT', 'OK'))


if __name__ == '__main__':
    unittest.main()
