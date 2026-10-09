"""Build explicit surveillance visuals. Dry-run by default; --apply enables AWS writes.

Uses an existing, least-privilege Snowflake QuickSight data source. This command
never creates subscriptions, users, data sources, or deletes resources. The
source is synthetic demo data; these visuals are not customer outcomes or ML.
Run --help for required account, principal, data-source and namespace arguments.
"""
import argparse
import json
import re
import time
from pathlib import Path


def identifier(value):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{0,127}', value):
        raise ValueError('Database must be a simple uppercase Snowflake identifier')
    return value


def column(dataset, name):
    return {'DataSetIdentifier': dataset, 'ColumnName': name}


def numerical(dataset, name, aggregation='AVERAGE'):
    return {'NumericalMeasureField': {
        'FieldId': f'{dataset}-{name.lower()}', 'Column': column(dataset, name),
        'AggregationFunction': {'SimpleNumericalAggregation': aggregation},
    }}


def title(text):
    return {'Visibility': 'VISIBLE', 'FormatText': {'PlainText': text}}


def build_requests(account, region, principal, source_arn, database, prefix):
    """Pure request construction: no credentials, network calls, or publication."""
    identifier(database)
    if not re.fullmatch(r'\d{12}', account):
        raise ValueError('Account must contain 12 digits')
    if not re.fullmatch(r'[a-z][a-z0-9-]{2,80}', prefix):
        raise ValueError('Use a unique resource prefix with lowercase letters, digits and hyphens')
    arn_base = f'arn:aws:quicksight:{region}:{account}'
    if not principal.startswith(arn_base + ':user/') or not source_arn.startswith(arn_base + ':datasource/'):
        raise ValueError('Principal and data source must belong to the selected account and region')
    specs = {
        'trend': {
            'sql': f'SELECT METRIC_DATE, ALERT_COUNT, CONFIRMED_COUNT, EVENT_COUNT FROM {database}.CURATED.TREND_ANALYSIS',
            'columns': [('METRIC_DATE', 'DATETIME'), ('ALERT_COUNT', 'INTEGER'), ('CONFIRMED_COUNT', 'INTEGER'), ('EVENT_COUNT', 'INTEGER')],
        },
        'accounts': {
            'sql': f'SELECT ENTITY_ID, ENTITY_NAME, REGION, CATEGORY, ALERT_PRECISION_PCT, NOTIONAL_USD, ALERT_COUNT, CONFIRMED_COUNT FROM {database}.CURATED.PERFORMANCE_SUMMARY',
            'columns': [('ENTITY_ID', 'STRING'), ('ENTITY_NAME', 'STRING'), ('REGION', 'STRING'), ('CATEGORY', 'STRING'), ('ALERT_PRECISION_PCT', 'DECIMAL'), ('NOTIONAL_USD', 'DECIMAL'), ('ALERT_COUNT', 'INTEGER'), ('CONFIRMED_COUNT', 'INTEGER')],
        },
        'risk': {
            'sql': (f'SELECT r.ENTITY_ID, p.CATEGORY, p.REGION, r.SUSPICION_PROB_7D, r.RISK_BAND, r.SCORED_AS_OF '
                    f'FROM {database}.ML.SUSPICION_RISK_SCORES r JOIN {database}.CURATED.PERFORMANCE_SUMMARY p ON p.ENTITY_ID = r.ENTITY_ID'),
            'columns': [('ENTITY_ID', 'STRING'), ('CATEGORY', 'STRING'), ('REGION', 'STRING'), ('SUSPICION_PROB_7D', 'DECIMAL'), ('RISK_BAND', 'STRING'), ('SCORED_AS_OF', 'DATETIME')],
        },
        'trades': {
            'sql': f'SELECT ACCOUNT_ID, EVENT_TS, NOTIONAL_USD, SELF_MATCH_PCT, STATUS FROM {database}.RAW.LIVE_TRADES',
            'columns': [('ACCOUNT_ID', 'STRING'), ('EVENT_TS', 'DATETIME'), ('NOTIONAL_USD', 'DECIMAL'), ('SELF_MATCH_PCT', 'DECIMAL'), ('STATUS', 'STRING')],
        },
    }
    datasets = []
    for name, spec in specs.items():
        datasets.append({
            'AwsAccountId': account, 'DataSetId': f'{prefix}-{name}',
            'Name': f'Exchange surveillance synthetic demo - {name}', 'ImportMode': 'DIRECT_QUERY',
            'PhysicalTableMap': {'source': {'CustomSql': {
                'DataSourceArn': source_arn, 'Name': name, 'SqlQuery': spec['sql'],
                'Columns': [{'Name': key, 'Type': kind} for key, kind in spec['columns']],
            }}},
            'Permissions': [{'Principal': principal, 'Actions': [
                'quicksight:DescribeDataSet', 'quicksight:DescribeDataSetPermissions',
                'quicksight:PassDataSet', 'quicksight:DescribeIngestion', 'quicksight:ListIngestions',
            ]}],
        })
    visuals = [
        {'KPIVisual': {
            'VisualId': 'account-count', 'Title': title('Accounts monitored (synthetic)'),
            'ChartConfiguration': {'FieldWells': {'Values': [{'CategoricalMeasureField': {
                'FieldId': 'accounts-distinct', 'Column': column('accounts', 'ENTITY_ID'),
                'AggregationFunction': 'DISTINCT_COUNT',
            }}]}},
        }},
        {'LineChartVisual': {
            'VisualId': 'daily-alerts', 'Title': title('Daily alerts raised - synthetic'),
            'ChartConfiguration': {'FieldWells': {'LineChartAggregatedFieldWells': {
                'Category': [{'DateDimensionField': {
                    'FieldId': 'trend-date', 'Column': column('trend', 'METRIC_DATE'), 'DateGranularity': 'DAY',
                }}],
                'Values': [numerical('trend', 'ALERT_COUNT', 'SUM'), numerical('trend', 'CONFIRMED_COUNT', 'SUM')],
            }}},
        }},
        {'BarChartVisual': {
            'VisualId': 'account-confirmed', 'Title': title('Confirmed suspicious alerts by account (90 days) - synthetic'),
            'ChartConfiguration': {'Orientation': 'HORIZONTAL', 'FieldWells': {'BarChartAggregatedFieldWells': {
                'Category': [{'CategoricalDimensionField': {
                    'FieldId': 'accounts-id', 'Column': column('accounts', 'ENTITY_ID'),
                }}],
                'Values': [numerical('accounts', 'CONFIRMED_COUNT', 'SUM')],
            }}, 'SortConfiguration': {'CategorySort': [{'FieldSort': {'FieldId': 'accounts-confirmed_count', 'Direction': 'DESC'}}]}},
        }},
        {'KPIVisual': {
            'VisualId': 'trade-events', 'Title': title('Firehose trade events loaded by Snowpipe'),
            'ChartConfiguration': {'FieldWells': {'Values': [{'CategoricalMeasureField': {
                'FieldId': 'trades-count', 'Column': column('trades', 'ACCOUNT_ID'), 'AggregationFunction': 'COUNT',
            }}]}},
        }},
        {'BarChartVisual': {
            'VisualId': 'suspicion-risk', 'Title': title('Next-7-day suspicious-activity probability by account (Snowflake ML) - synthetic'),
            'ChartConfiguration': {'Orientation': 'HORIZONTAL', 'FieldWells': {'BarChartAggregatedFieldWells': {
                'Category': [{'CategoricalDimensionField': {
                    'FieldId': 'risk-id', 'Column': column('risk', 'ENTITY_ID'),
                }}],
                'Values': [numerical('risk', 'SUSPICION_PROB_7D', 'MAX')],
            }}, 'SortConfiguration': {'CategorySort': [{'FieldSort': {'FieldId': 'risk-suspicion_prob_7d', 'Direction': 'DESC'}}]}},
        }},
    ]
    dashboard = {
        'AwsAccountId': account, 'DashboardId': f'{prefix}-dashboard',
        'Name': 'Digital asset exchange surveillance - synthetic on-demand snapshot',
        'Definition': {
            'DataSetIdentifierDeclarations': [
                {'Identifier': name, 'DataSetArn': f'{arn_base}:dataset/{prefix}-{name}'} for name in specs
            ],
            'Sheets': [{
                'SheetId': 'overview', 'Name': 'Surveillance overview', 'Visuals': visuals,
                'Layouts': [{'Configuration': {'GridLayout': {'Elements': [
                    {'ElementId': 'account-count', 'ElementType': 'VISUAL', 'ColumnIndex': 0, 'RowIndex': 0, 'ColumnSpan': 12, 'RowSpan': 3},
                    {'ElementId': 'trade-events', 'ElementType': 'VISUAL', 'ColumnIndex': 12, 'RowIndex': 0, 'ColumnSpan': 12, 'RowSpan': 3},
                    {'ElementId': 'daily-alerts', 'ElementType': 'VISUAL', 'ColumnIndex': 0, 'RowIndex': 3, 'ColumnSpan': 36, 'RowSpan': 8},
                    {'ElementId': 'account-confirmed', 'ElementType': 'VISUAL', 'ColumnIndex': 0, 'RowIndex': 11, 'ColumnSpan': 18, 'RowSpan': 12},
                    {'ElementId': 'suspicion-risk', 'ElementType': 'VISUAL', 'ColumnIndex': 18, 'RowIndex': 11, 'ColumnSpan': 18, 'RowSpan': 12},
                ]}}}],
            }],
        },
        'Permissions': [{'Principal': principal, 'Actions': [
            'quicksight:DescribeDashboard', 'quicksight:ListDashboardVersions', 'quicksight:QueryDashboard',
        ]}],
    }
    topic = {
        'AwsAccountId': account, 'TopicId': f'{prefix}-topic',
        'Topic': {
            'Name': 'Digital asset exchange surveillance synthetic observations',
            'Description': 'On-demand synthetic snapshots of 40 accounts on a fictional digital-asset exchange. Includes Snowflake ML next-7-day suspicious-activity risk. No causal or customer-outcome claims.',
            'DataSets': [{
                'DatasetArn': f'{arn_base}:dataset/{prefix}-accounts',
                'DatasetName': 'Account observations',
                'Columns': [
                    {'ColumnName': key, 'ColumnFriendlyName': key.replace('_', ' ').title(),
                     'ColumnDataRole': 'MEASURE' if kind in ('INTEGER', 'DECIMAL') else 'DIMENSION',
                     **({'Aggregation': 'AVERAGE', 'NonAdditive': True} if key == 'ALERT_PRECISION_PCT' else {})}
                    for key, kind in specs['accounts']['columns']
                ],
            }, {
                'DatasetArn': f'{arn_base}:dataset/{prefix}-risk',
                'DatasetName': 'Suspicious-activity risk scores',
                'DatasetDescription': 'Snowflake ML classification: probability of confirmed suspicious activity in the next 7 days, per account.',
                'Columns': [
                    {'ColumnName': key, 'ColumnFriendlyName': key.replace('_', ' ').title(),
                     'ColumnDataRole': 'MEASURE' if kind in ('INTEGER', 'DECIMAL') else 'DIMENSION',
                     **({'Aggregation': 'MAX', 'NonAdditive': True} if key == 'SUSPICION_PROB_7D' else {})}
                    for key, kind in specs['risk']['columns']
                ],
            }],
        },
    }
    return {'datasets': datasets, 'dashboard': dashboard, 'topic': topic}


def validate_requests(requests):
    """Validate against the installed AWS API schema without issuing requests."""
    import botocore.session
    from botocore.validate import validate_parameters
    model = botocore.session.get_session().get_service_model('quicksight')
    for request in requests['datasets']:
        validate_parameters(request, model.operation_model('CreateDataSet').input_shape)
    validate_parameters(requests['dashboard'], model.operation_model('CreateDashboard').input_shape)
    validate_parameters(requests['topic'], model.operation_model('CreateTopic').input_shape)


def wait_dashboard(client, account, dashboard_id, version, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = client.describe_dashboard(AwsAccountId=account, DashboardId=dashboard_id, VersionNumber=version)
        current = result['Dashboard']['Version']
        status = current['Status']
        if status in ('CREATION_SUCCESSFUL', 'UPDATE_SUCCESSFUL'):
            return
        if status.endswith('FAILED') or status == 'DELETED':
            raise RuntimeError(f'Dashboard version failed: {status}; inspect describe-dashboard errors')
        time.sleep(3)
    raise TimeoutError('Dashboard creation did not complete; do not treat it as published')


def apply_requests(session, requests, update=False, with_topic=False):
    account = requests['dashboard']['AwsAccountId']
    if session.client('sts').get_caller_identity()['Account'] != account:
        raise RuntimeError('AWS identity mismatch; no writes attempted')
    client = session.client('quicksight')
    # Preflight both dependencies before any writes.
    principal = requests['dashboard']['Permissions'][0]['Principal']
    namespace, username = principal.split(':user/', 1)[1].split('/', 1)
    client.describe_user(AwsAccountId=account, Namespace=namespace, UserName=username)
    source = requests['datasets'][0]['PhysicalTableMap']['source']['CustomSql']['DataSourceArn']
    client.describe_data_source(AwsAccountId=account, DataSourceId=source.rsplit('/', 1)[1])
    for request in requests['datasets']:
        try:
            client.create_data_set(**request)
        except client.exceptions.ResourceExistsException:
            if not update:
                raise RuntimeError('Resource exists. Choose a new prefix or explicitly use --update')
            client.update_data_set(**{key: value for key, value in request.items() if key != 'Permissions'})
    dashboard = requests['dashboard']
    try:
        created = client.create_dashboard(**dashboard)
    except client.exceptions.ResourceExistsException:
        if not update:
            raise RuntimeError('Dashboard exists; update requires --update')
        created = client.update_dashboard(**{key: value for key, value in dashboard.items() if key != 'Permissions'})
    version = int(created['VersionArn'].rsplit('/', 1)[1])
    wait_dashboard(client, account, dashboard['DashboardId'], version)
    client.update_dashboard_published_version(AwsAccountId=account, DashboardId=dashboard['DashboardId'], VersionNumber=version)
    published = client.describe_dashboard(AwsAccountId=account, DashboardId=dashboard['DashboardId'])
    if published['Dashboard']['Version']['VersionNumber'] != version:
        raise RuntimeError('Published dashboard version does not match the expected version')
    topic_result = None
    if with_topic:
        try:
            topic_result = client.create_topic(**requests['topic'])
        except client.exceptions.ResourceExistsException:
            if not update:
                raise RuntimeError('Topic exists; update requires --update')
            topic_result = client.update_topic(**requests['topic'])
    return {'dashboard_id': dashboard['DashboardId'], 'published_version': version,
            'topic_requested': with_topic, 'topic_refresh_arn': (topic_result or {}).get('RefreshArn'),
            'rendered_visual_validation': 'NOT_TESTED', 'q_answer_validation': 'NOT_TESTED'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--account', required=True)
    parser.add_argument('--region', default='us-west-2')
    parser.add_argument('--profile', default='default')
    parser.add_argument('--principal-arn', required=True)
    parser.add_argument('--data-source-arn', required=True)
    parser.add_argument('--database', required=True)
    parser.add_argument('--prefix', required=True, help='Unique resource prefix for this demo')
    parser.add_argument('--apply', action='store_true', help='Enable guarded AWS writes; otherwise just print definitions')
    parser.add_argument('--update', action='store_true', help='Explicitly authorize updating resources under this prefix')
    parser.add_argument('--with-topic', action='store_true', help='Create/update a Q topic; requires enabled Q access')
    args = parser.parse_args()
    requests = build_requests(args.account, args.region, args.principal_arn, args.data_source_arn, args.database, args.prefix)
    validate_requests(requests)
    if args.apply:
        import boto3
        print(json.dumps(apply_requests(boto3.Session(profile_name=args.profile, region_name=args.region), requests, args.update, args.with_topic), indent=2))
    else:
        print(json.dumps(requests, indent=2))


if __name__ == '__main__':
    main()
