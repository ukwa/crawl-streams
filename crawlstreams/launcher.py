#!/usr/bin/env python
# encoding: utf-8
'''
crawlstreams.launcher

@contact:    Andrew.Jackson@bl.uk
'''

import os
import sys
import time
import logging
import argparse
from crawlstreams.enqueue import KafkaLauncher


# Set up a logging handler:
handler = logging.StreamHandler()
# handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s")
handler.setFormatter(formatter)

# Attach to root logger
logging.root.addHandler(handler)

# Set default logging output for all modules.
logging.root.setLevel(logging.WARNING)

# Set logging for this module and keep the reference handy:
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def sender(launcher, args, uri):
    # Ensure a http:// or https:// at the front:
    if not (uri.startswith("http://") or uri.startswith("https://")):
        uri = "http://%s" % uri

    # Add the main URL
    launcher.launch(uri, args.source, isSeed=args.seed, forceFetch=args.forceFetch,
                    recrawl_interval=args.recrawl_interval, sheets=args.sheets, reset_quotas=args.reset_quotas,
                    webrender_this=args.webrender_this, launch_ts=args.launch_ts, parallel_queues=args.parallel_queues)


def main(argv=None):
    parser = argparse.ArgumentParser('(Re)Launch URIs into crawl queues.')
    parser.add_argument('-k', '--kafka-bootstrap-server', dest='kafka_server', type=str, default="localhost:9092",
                        help="Kafka bootstrap server(s) to use [default: %(default)s]")
    parser.add_argument("-L", "--launch-datetime", dest="launch_dt", default=None, required=False, type=str,
                        help="Launch request timestamp as 14-character datetime e.g. '20190301120000' or use 'now' to use the current time. [default: %(default)s]")
    parser.add_argument('queue', help="Name of queue to send URIs too, e.g. 'fc.tocrawl.npld'.")
    parser.add_argument('crawl_feed_file', help="Crawl feed file, containing crawl job definitions.")

    args = parser.parse_args()

    # Set up launcher:
    launcher = KafkaLauncher(kafka_server=args.kafka_server, topic=args.queue)

    # Read from a file, if the input is a file:
    if os.path.isfile(args.uri_or_filename):
        with open(args.uri_or_filename,'r') as f:
            for line in f:
                sent = False
                while not sent:
                    try:
                        uri = line.strip()
                        sender(launcher, args, uri)
                        sent = True
                    except Exception as e:
                        logger.error("Exception while submitting: %s" % line)
                        logger.exception(e)
                        logger.info("Sleeping for ten seconds...")
                        time.sleep(10)
    else:
        # Or send one URI
        sender(launcher, args, args.uri_or_filename)

    # Wait for send to complete:
    launcher.flush()


if __name__ == "__main__":
    sys.exit(main())
