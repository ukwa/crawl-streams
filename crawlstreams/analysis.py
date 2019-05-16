import os
import sys
import json
import copy
import logging
import argparse
import tempfile
import threading
from urllib.parse import urlsplit
import time
from kafka import KafkaConsumer
from threading import Thread
from collections import OrderedDict, deque

logger = logging.getLogger(__name__)


class LimitedSizeDict(OrderedDict):
  def __init__(self, *args, **kwds):
    self.size_limit = kwds.pop("size_limit", None)
    OrderedDict.__init__(self, *args, **kwds)
    self._check_size_limit()

  def __setitem__(self, key, value):
    OrderedDict.__setitem__(self, key, value)
    self._check_size_limit()

  def _check_size_limit(self):
    if self.size_limit is not None:
      while len(self) > self.size_limit:
        self.popitem(last=False)


class CrawlLogConsumer(Thread):
    '''
    {
      "mimetype": "image/vnd.microsoft.icon",
      "content_length": 4150,
      "via": "http://dublincore.org/robots.txt",
      "warc_offset": 1128045,
      "thread": 2,
      "url": "http://dublincore.org/favicon.ico",
      "status_code": 200,
      "start_time_plus_duration": "20180907204730677+225",
      "host": "dublincore.org",
      "seed": "",
      "content_digest": "sha1:HYZOZGARLB7ATJSLRIGZN2UEORPIZXJH",
      "hop_path": "LLEPI",
      "warc_filename": "BL-NPLD-TEST-20180907204622099-00000-11601~h3w~8443.warc.gz",
      "crawl_name": "npld-fc",
      "size": 4415,
      "timestamp": "2018-09-07T20:47:30.913Z",
      "annotations": "ip:139.162.252.71",
      "extra_info": {
        "scopeDecision": "ACCEPT by rule #2 MatchesRegexDecideRule",
        "warcFileRecordLength": 3069
      }
    }
    '''

    def __init__(self, kafka_topic, kafka_brokers, group_id, from_beginning='false'):
        Thread.__init__(self)
        # Ensure we don't hang around...
        self.setDaemon(True)
        # Remember settings
        self.kafka_topic = kafka_topic
        self.kafka_brokers = kafka_brokers
        self.group_id = group_id
        self.from_beginning = from_beginning
        # The last event timestamp we saw
        self.last_timestamp = None
        # Details of the most recent screenshots:
        self.screenshots = deque(maxlen=os.environ.get("MAX_SCREENSHOTS_MEMORY", 100))
        self.screenshotsLock = threading.Lock()
        # This is used to hold the last 1000 messages, for tail analysis
        self.recent = deque(maxlen=os.environ.get("MAX_LOG_MEMORY", 10000))
        self.recentLock = threading.Lock()
        # Information on the most recent hosts:
        self.hosts = LimitedSizeDict(size_limit=os.environ.get("MAX_HOSTS_MEMORY", 500))
        self.hostsLock = threading.Lock()

    def process_message(self, message):
        try:
            m = json.loads(message.value)

            # Record time event and latest timestamp
            url = m['url']
            with self.recentLock:
                self.recent.append(m)
            self.last_timestamp = m['timestamp']

            # Recent screenshots:
            if url.startswith('screenshot:'):
                with self.screenshotsLock:
                    original_url = url[11:]
                    # It appears PDFs sent to PhantomJS lead to empty records:
                    if original_url == '':
                        logger.info("Found empty screenshot url %s" % m)
                    else:
                        self.screenshots.append((original_url, m['timestamp']))

            # Host info:
            host = self.get_host(url)
            if host:
                with self.hostsLock:
                    hs = self.hosts.get(host, None)
                    if hs is None:
                        hs = {}
                        hs['stats'] = {}
                        hs['stats']['first_timestamp'] = m['timestamp']
                        hs['content_types'] = {}
                        hs['status_codes'] = {}
                        hs['via'] = {}
                        self.hosts[host] = hs

                    # Basics
                    hs['stats']['last_timestamp'] = m['timestamp']
                    hs['stats']['total'] = hs['stats'].get('total', 0) + 1

                    # Mime types:
                    mimetype = m.get('mimetype', None)
                    if not mimetype:
                        mimetype = m.get('content_type', None)
                    if not mimetype:
                        mimetype = 'unknown-content-type'
                    hs['content_types'][mimetype] = hs['content_types'].get(mimetype, 0) + 1

                    # Status Codes:
                    sc = str(m.get('status_code'))
                    if not sc:
                        #print(json.dumps(m, indent=2))
                        sc = "-"
                    hs['status_codes'][sc] = hs['status_codes'].get(sc, 0) + 1

                    # Via
                    via_host = self.get_host(m.get('via', None))
                    if via_host and host != via_host:
                        hs['via'][via_host] = hs['via'].get(via_host, 0) + 1

        except Exception as e:
            logger.exception("Could not process message %s" % message)

    def get_host(self, url):
        if url is None:
            return None
        parts = urlsplit(url)
        return parts[1]

    def get_status_codes(self):
        status_codes = {}
        with self.recentLock:
            for m in self.recent:
                sc = str(m.get('status_code'))
                if sc:
                    status_codes[sc] = status_codes.get(sc, 0) + 1
        # Sort by count:
        status_codes = sorted(status_codes.items(), key=lambda x: x[1], reverse=True)
        return status_codes

    def get_stats(self):
        # Get screenshots sorted by timestamp
        with self.screenshotsLock:
            shots = list(self.screenshots)
            shots.sort(key=lambda shot: shot[1], reverse=True)
        with self.hostsLock:
            # Make a copy while locked:
            hosts = copy.deepcopy(dict(self.hosts))
        return {
            'last_timestamp': self.last_timestamp,
            'status_codes': self.get_status_codes(),
            'screenshots': shots,
            'hosts': hosts
        }

    def run(self):
        # Set up a consumer:
        up = False
        while not up:
            try:
                self.consumer = KafkaConsumer(self.kafka_topic, bootstrap_servers=self.kafka_brokers, group_id=self.group_id)
                # If requested, start at the start:
                if self.from_beginning and self.from_beginning.lower() == 'true':
                    logger.info("Seeking to the beginning of %s" % self.kafka_topic)
                    self.consumer.poll(timeout_ms=5000)
                    self.consumer.seek_to_beginning()
                up = True
            except Exception as e:
                logger.exception("Failed to start CrawlLogConsumer!")
                time.sleep(5)

        # And consume...
        for message in self.consumer:
            self.process_message(message)


def main():
    parser = argparse.ArgumentParser('(Re)Launch URIs into crawl queues.')
    parser.add_argument('-k', '--kafka-bootstrap-server', dest='bootstrap_server', type=str, default="localhost:9092",
                        help="Kafka bootstrap server(s) to use [default: %(default)s]")
    parser.add_argument("-B", "--seek-to-beginning", dest="seek_to_beginning", action="store_true", default=False, required=False,
                        help="Start with the earliest messages rather than from the latest. [default: %(default)s]")
    parser.add_argument("-t", "--topic", dest="topic", default="fc.crawled", required=False,
                        help="Name of queue to inspect. [default: %(default)s]")
    parser.add_argument("-u", "--update-interval", dest="update_interval", default=10, required=False, type=int,
                        help="How often to update the output files (in seconds). [default: %(default)s]")
    parser.add_argument("-o", "--output", dest="output", default="fc.crawled.json", required=False,
                        help="Output file to create and update. [default: %(default)s]")


    # Parse the args:
    args = parser.parse_args()

    # Set up the crawl log sampler
    kafka_broker = args.bootstrap_server
    kafka_crawled_topic = args.topic
    kafka_seek_to_beginning = args.seek_to_beginning
    consumer = CrawlLogConsumer(
        kafka_crawled_topic, [kafka_broker], None,
        from_beginning=kafka_seek_to_beginning)
    consumer.start()

    # Emit:
    while True:
        time.sleep(args.update_interval)
        stats = consumer.get_stats()
        print("Updating %s last timestamp %s" % (args.output, stats.get('last_timestamp', None)))
        # Write to temp file:
        temp_name = "%s.tmp" % args.output
        with open(temp_name, 'w') as outfile:
            json.dump(stats, outfile, indent=2)
        # Atomic replace:
        os.rename(temp_name, args.output)


if __name__ == "__main__":
    sys.exit(main())
