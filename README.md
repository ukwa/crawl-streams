Crawl Streams
=============

Tools for operating on the event streams relating to our crawler activity.


## Development Setup

You'll need Snappy and build tools. e.g. for RHEL/CentOS:

```
sudo yum install snappy-devel
sudo yum install gcc gcc-c++ libtool # Maybe this <<<
```

With these in place, this should work:

```
git clone https://github.com/ukwa/crawl-streams.git
cd crawl-streams/
virtualenv -p python3.7 venv
source venv/bin/activate
python setup.py install
```

### Test Services

The [docker-compose.yml](./docker-compose.yml) file is intended to be used to spin-up local versions of Kafka suitable for developing against.

This still needs work...

- [ ] Check the test services are what we need, and only what we need (should be Kafka only I think?)
- [ ] Supply test data and scripts/instructions to populate the test services.
