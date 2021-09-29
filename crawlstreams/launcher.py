#!/usr/bin/env python
# encoding: utf-8
'''
crawlstreams.launcher

@contact:    Andrew.Jackson@bl.uk
'''

import os
import sys
import time
import json
import logging
import argparse
from crawlstreams.enqueue import KafkaLauncher
from datetime import datetime, timezone, timedelta

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

class Launcher():

    def __init__(self, args):
        self.input_file = args.crawl_feed_file
        self.kafka_server = args.kafka_server
        self.queue = args.queue

    def run(self, now=None):
        # Set up launcher:
        self.launcher = KafkaLauncher(kafka_server=self.kafka_server, topic=self.queue)

        # Get current time
        if not now or now == 'now':
            #now = datetime.now(tz=timezone.utc)
            now = datetime.now()
        logger.debug("Now timestamp: %s" % str(now))

        # Process looking for due:
        self.i_launches = 0
        self.target_errors = 0
        for t in self._all_targets():
            logger.debug("----------")
            logger.debug("Looking at %s (tid:%d)" % (t['title'], t['id']))

            # Look out for problems:
            if len(t['seeds']) == 0:
                logger.error("This target has no seeds! tid: %d" % t['id'])
                self.target_errors += 1
                continue

            # Add a source tag if this is a watched target:
            source = "tid:%d:%s" % (t['id'], t['seeds'][0])

            # Check the scheduling:
            for schedule in t['schedules']:
                # Skip if target schedule outside of start/end range
                if schedule['startDate']:
                    startDate = datetime.strptime(schedule['startDate'], "%Y-%m-%d %H:%M:%S")
                    logger.debug("Target schedule start date: %s" % str(startDate))
                    if (now < startDate):
                        logger.debug("Start date %s not yet reached" % startDate)
                        continue
                else:
                    logger.debug("Skipping target schedule start date: %s" % schedule['startDate'])
                    continue
                endDate = 'N/S'
                if schedule['endDate']:
                    endDate = datetime.strptime(schedule['endDate'], "%Y-%m-%d %H:%M:%S")
                    if now > endDate:
                        logger.debug("End date %s passed" % endDate)
                        continue
                logger.debug("Target schedule end date:  %s" % str(endDate))
                logger.debug("Target frequency: %s" % schedule['frequency'])

                # Check if the frequency and date match up:
                if schedule['frequency'] == "DAILY":
                    self.launch_by_hour(now, startDate, endDate, t, source, 'DAILY')

                elif schedule['frequency'] == "WEEKLY":
                    if now.isoweekday() == startDate.isoweekday():
                        self.launch_by_hour(now, startDate, endDate, t, source, 'WEEKLY')
                    else:
                        logger.debug("WEEKLY: isoweekday %s differs from schedule %s" % (
                            now.isoweekday(), startDate.isoweekday()))

                elif schedule['frequency'] == "MONTHLY":
                    if now.day == startDate.day:
                        self.launch_by_hour(now, startDate, endDate, t, source, 'MONTHLY')
                    else:
                        logger.debug("MONTHLY: date %s does not match schedule %s" % (
                            now, startDate))
                        logger.debug("MONTHLY: day %s differs from schedule %s" % (now.day, startDate.day))

                elif schedule['frequency'] == "QUARTERLY":
                    if now.day == startDate.day and now.month % 3 == startDate.month % 3:
                        self.launch_by_hour(now, startDate, endDate, t, source, 'QUARTERLY')
                    else:
                        logger.debug("QUARTERLY: date %s does not match schedule %s" % (
                            now, startDate))
                        logger.debug(
                            "QUARTERLY: month3 %s versus schedule %s" % (now.month % 3, startDate.month % 3))

                elif schedule['frequency'] == "SIXMONTHLY":
                    if now.day == startDate.day and now.month % 6 == startDate.month % 6:
                        self.launch_by_hour(now, startDate, endDate, t, source, 'SIXMONTHLY')
                    else:
                        logger.debug("SIXMONTHLY: date %s does not match schedule %s" % (
                            now, startDate))
                        logger.debug(
                            "SIXMONTHLY: month6 %s versus schedule %s" % (now.month % 6, startDate.month % 6))

                elif schedule['frequency'] == "ANNUAL":
                    if now.day == startDate.day and now.month == startDate.month:
                        self.launch_by_hour(now, startDate, endDate, t, source, 'ANNUAL')
                    else:
                        logger.debug("ANNUAL: date %s does not match schedule %s" % (
                            now, startDate))
                        logger.debug("ANNUAL: month %s versus schedule %s" % (now.month, startDate.month))
                elif schedule['frequency'] == "DOMAINCRAWL":
                    logger.debug("Skipping crawl frequency " + schedule['frequency'])
                else:
                    logger.error("Don't understand crawl frequency " + schedule['frequency'])

        logger.info("Closing the launcher to ensure everything is pushed to Kafka...")
        self.launcher.flush()
        #self.launcher.close()

        logger.info("Completed. Launches this hour: %s" % self.i_launches)

    def _all_targets(self):
        with open(self.input_file,'r') as fin:
            for line in fin:
                item = json.loads(line)
                yield item

    def get_metrics(self, registry):
        # type: (CollectorRegistry) -> None

        g = Gauge('ukwa_seeds_launched',
                  'Total number of seeds launched.',
                  labelnames=['stream'], registry=registry)
        g.labels(stream=self.frequency).set(self.i_launches)

        g = Gauge('ukwa_target_errors',
                  'Total number of targets that appear malformed.',
                  labelnames=['stream'], registry=registry)
        g.labels(stream=self.frequency).set(self.target_errors)

    def launch_by_hour(self, now, startDate, endDate, t, source, freq):
        # Is it the current hour?
        if now.hour is startDate.hour:
            logger.info(
                "%s target %s (tid: %s) scheduled to crawl (now: %s, start: %s, end: %s), sending to FC-3-uris-to-crawl" % (
                freq, t['title'], t['id'], now, startDate, endDate))
            counter = 0
            for seed in t['seeds']:
                # Assume all are true seeds, which will over-crawl where sites have aliases that are being treated as seeds.
                # TODO Add handling of aliases
                isSeed = True

                # Set-up sheets
                sheets = []

                # Robots.txt
                if t['ignoreRobotsTxt']:
                    sheets.append('ignoreRobots')
                # Scope
                if t['scope'] == 'subdomains':
                    sheets.append('subdomainsScope')
                elif t['scope'] == 'plus1Scope':
                    sheets.append('plus1Scope')
                # Limits
                if t['depth'] == 'CAPPED_LARGE':
                    sheets.append('higherLimit')
                elif t['depth'] == 'DEEP':
                    sheets.append('noLimit')

                # Set up the launch_ts: (Should be startDate but if that happens to be in the future this will all break)
                launch_timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime(time.mktime(now.timetuple())))
                
                # How many parallel queues:
                parallel_queues = 1
                if 'twitter.com' in seed:
                    parallel_queues = 2

                # And send launch message, always resetting any crawl quotas:
                self.launcher.launch(seed, source, isSeed, forceFetch=True, sheets=sheets, 
                    reset_quotas=True, launch_ts=launch_timestamp, 
                    inherit_launch_ts=False,parallel_queues=parallel_queues)
                counter = counter + 1
                self.i_launches = self.i_launches + 1

        else:
            logger.debug("The hour (%s) is not current." % startDate.hour)


def main(argv=None):
    parser = argparse.ArgumentParser('(Re)Launch URIs into crawl queues.')
    parser.add_argument('-k', '--kafka-bootstrap-server', dest='kafka_server', type=str, default="localhost:9092",
                        help="Kafka bootstrap server(s) to use [default: %(default)s]")
    parser.add_argument("-L", "--launch-datetime", dest="launch_dt", default='now', required=False, type=str,
                        help="Launch request datetime e.g. '2019-03-01T12:00:00Z' or use 'now' to use the current time. [default: %(default)s]")
    parser.add_argument('queue', help="Name of queue to send URIs too, e.g. 'fc.tocrawl.npld'.")
    parser.add_argument('crawl_feed_file', help="Crawl feed file, containing crawl job definitions.")

    args = parser.parse_args()

    # Configure the launcher based on the args:
    cl = Launcher(args)

    # Set the launch timestamp:
    if args.launch_dt != 'now':
        launch_datetime = datetime.fromisoformat(args.launch_dt)
    else:
        launch_datetime = 'now'

    # Run the launcher:
    cl.run(launch_datetime)

if __name__ == "__main__":
    sys.exit(main())
