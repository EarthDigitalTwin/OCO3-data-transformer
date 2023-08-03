import argparse
import json
import logging
from datetime import datetime, timezone
from time import sleep

import pika
import requests
from yaml import dump

from process_history.History import LocalHistory

try:
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Dumper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] [%(name)s::%(lineno)d] %(message)s'
)

logger = logging.getLogger('cmr_harvest')

URL = 'https://cmr.earthdata.nasa.gov/search/granules.json'
history = LocalHistory('test_history.json')

JOB_SIZE = 8


def harvest():
    params = {
        'collection_concept_id': 'C2237732007-GES_DISC',
        'page_size': 150
    }

    to_process = []

    logger.info('Fetching first CMR page')

    response = requests.get(URL, params=params)
    response.raise_for_status()
    entries = response.json()['feed']['entry']

    logger.info(f'CMR found {int(response.headers["cmr-hits"]):,} granules to page through')

    to_process.extend(history.check(
        [([l['href'] for l in e['links'] if l['href'].startswith('s3://')][0], e['updated']) for e in entries]
    ))

    while 'cmr-search-after' in response.headers:
        logger.info(f'Getting CMR search after page{response.headers["cmr-search-after"]}')

        response = requests.get(URL, params=params, headers={'cmr-search-after': response.headers['cmr-search-after']})
        response.raise_for_status()

        entries = response.json()['feed']['entry']

        to_process.extend(history.check(
            [([l['href'] for l in e['links'] if l['href'].startswith('s3://')][0], e['updated']) for e in entries]
        ))

        sleep(.15)

    logger.info(f'Found {len(to_process)} granules to process')

    to_process.sort(key=lambda x: x[0])

    return to_process


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-r', '--rmq',
        dest='rmq',
        required=True,
        help='RMQ hostname',
        metavar='HOST'
    )

    parser.add_argument(
        '--port',
        dest='port',
        required=False,
        default=5672,
        type=int,
        help='RMQ port',
        metavar='PORT'
    )

    parser.add_argument(
        '-u', '--username',
        dest='user',
        required=False,
        default='guest',
        help='RMQ username',
        metavar='USERNAME'
    )

    parser.add_argument(
        '-p', '--pass',
        dest='password',
        required=False,
        default='guest',
        help='RMQ password',
        metavar='PASSWORD'
    )

    parser.add_argument(
        '-q', '--queue',
        dest='queue',
        required=False,
        default='oco3',
        help='RMQ queue to publish to',
        metavar='QUEUE'
    )

    args = parser.parse_args()

    creds = pika.PlainCredentials(args.user, args.password)

    host = args.rmq
    port = args.port

    params = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=creds,
        heartbeat=60
    )

    channel = None

    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)

        channel.queue_declare(args.queue, durable=True,)
    except:
        print('Could not connect to RMQ')
        exit(1)

    granules = harvest()

    print(json.dumps(granules, indent=4))

    now = datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S') + 'Z'

    for batch in [granules[i:i+JOB_SIZE] for i in range(0, len(granules), JOB_SIZE)]:
        msg_dict = {'inputs': [g[0] for g in batch]}

        msg = dump(msg_dict, Dumper=Dumper).encode('utf-8')

        channel.basic_publish(exchange='', routing_key=args.queue, body=msg)

        publish = [(g[0], now) for g in batch]

        history.publish(publish)


if __name__ == '__main__':
    main()
