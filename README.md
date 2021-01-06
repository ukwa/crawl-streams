Crawl Streams
=============

Tools for operating on the event streams relating to our crawler activity.


## Development Setup

### Outside Docker

To develop directly on your host machine, you'll need Snappy and build tools. e.g. for RHEL/CentOS:

```
sudo yum install snappy-devel
sudo yum install gcc gcc-c++ libtool
```

With these in place, this should work:

```
git clone https://github.com/ukwa/crawl-streams.git
cd crawl-streams/
virtualenv -p python3.7 venv
source venv/bin/activate
python setup.py install
```

#### Supporting services

The provided [docker-compose.yml](./docker-compose.yml) file is intended to be used to spin-up local versions of Kafka suitable for developing against. Run 

```
$ docker-compose up -d kafka
```

And a Kafka service should be running on host port 9094. Kafka has it's own protocol, not HTTP, so you can't talk to it via curl etc. However, there is also a generic Kafka UI you can run like this:

```
$  docker-compose up -d ui
```

At which point you should be able to visit port 9990 (e.g. http://dev1.n45.wa.bl.uk:9990/) and have a look around.

#### Running the development version

You should now be able to edit the Python source files and run them to test against the given Kafka service. For example, to submit a URL to the NPLD frequent crawl's to-crawl topic, you can run:

```
$ python -m crawlstreams.submit -k dev1:9094 fc.tocrawl.npld http://a-test-string.com
```

To run the reporting script to analyse the contents of the `fc.crawled` topic, you use:

```
$ python -m crawlstreams.report -k dev1:9094 -t -1 -q fc.crawled
```

But this topic will need populating with test data. You can use Kafka's own tools to pull some data from the live service, like this:

    $ docker run -i --net host wurstmeister/kafka:2.12-2.1.0 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server crawler05.n45.bl.uk:9094 --topic fc.crawled --max-messages 100 > messages.json

You now have 100 messages from the live system in a JSON file. You can submit this to the local Kakfa like this:

    $ cat messages.json | docker run -i --net host wurstmeister/kafka:2.12-2.1.0 /opt/kafka/bin/kafka-console-producer.sh --broker-list dev1:9094 --topic fc.crawled

#### Running on live data

When performing read operations, it's fine to run against the live system. e.g. to see the raw `fc.crawled` feed from the live Crawler05 instance, you can use:

```
$  python -m crawlstreams.report -k crawler05.n45.bl.uk:9094 -t -1 -q fc.crawled -r | head
```

