# -*- coding: utf-8 -*-
#
# Copyright 2015, Jianing Yang
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
# Author: Jianing Yang <jianingy.yang@gmail.com>
#
from celery import Celery
from celery.bin import worker as celery_worker
from celery.result import ResultSet
from oslo.config import cfg
from yaml import load as yaml_load

import netsnmp
import logging
import telnetlib
import time
import sys

cli_opts = [
    cfg.BoolOpt('debug', default=False, help='enable debug'),
    cfg.StrOpt('graphite-host', default='monitor.corp.daling.com',
               help='graphite host'),
    cfg.IntOpt('graphite-port', default=2929,
               help='graphite port'),
    cfg.StrOpt('task', default='conf/default.yml',
               help='task configuration'),
    cfg.IntOpt('timeout', default=30,
               help='timeout in seconds'),
    cfg.StrOpt('broker', default='amqp://mypush:mypush@deb01/mypush',
               help='celery broker connection string'),
    cfg.IntOpt('num_workers', default=8,
               help='# of workers'),
]

CONF = cfg.CONF
CONF.register_cli_opts(cli_opts)
LOG = logging.getLogger(__name__)

celery_app = Celery('snmp_collector', backend='amqp', serializer='yaml')


def setup():
    CONF(sys.argv[1:])
    log_level = logging.DEBUG if CONF.debug else logging.INFO
    log_format = ('%(asctime)s|[%(levelname)s] #%(process)d'
                  ' (%(name)s) %(message)s')
    logging.basicConfig(level=log_level,
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S')
    celery_app.conf.BROKER_URL = CONF.broker
    celery_app.conf.CELERY_TASK_SERIALIZER = 'yaml'
    celery_app.conf.CELERY_ACCEPT_CONTENT = ['json', 'yaml']


def reload_task():
    y = yaml_load(file(CONF.task, 'r').read())
    assert 'hosts' in y and isinstance(y['hosts'], list)
    assert 'metrics' in y and isinstance(y['metrics'], list)

    hosts = dict(map(lambda x: (x['host'], x), y['hosts']))
    metrics = dict(map(lambda x: (x['metric'], x), y['metrics']))

    return hosts, metrics


def write_graphite(messages):
    LOG.info("connecting to %s:%s" % (CONF.graphite_host, CONF.graphite_port))
    tn = telnetlib.Telnet(CONF.graphite_host, CONF.graphite_port)
    for message in messages:
        LOG.info("sending: '%s'" % message)
        tn.write(message + "\n")
    tn.close()


@celery_app.task(acks_late=True)
def collect_metric(metric, hosts):
    host = hosts[metric['host']]
    var = netsnmp.Varbind(metric['oid'])
    res = netsnmp.snmpget(var, Version=int(host.get('version', 1)),
                          DestHost=host.get('host'),
                          Timeout=CONF.timeout,
                          Community=host.get('community', 'public'))
    message = "%s %s %d" % (metric['metric'], res[0], time.time())
    return message


def run_scheduler():
    setup()
    hosts, metrics = reload_task()

    workers = []
    collect_metric.broker = CONF.broker
    collect_metric.time_limit = CONF.timeout
    for metric in metrics.values():
        try:
            worker = collect_metric.apply_async(args=[metric, hosts], broker=CONF.broker)
            workers.append(worker)
        except Exception as e:
            print "ERROR: %s" % e

    messages = []

    def _acc(task_id, value):
        messages.append(str(value))

    ResultSet(workers).get(timeout=CONF.timeout,
                           propagate=False,
                           callback=_acc)
    write_graphite(messages)


def run_worker():
    from celery.task.control import discard_all

    setup()
    worker = celery_worker.worker(app=celery_app)
    if CONF.debug:
        traceback = True
        loglevel = 'DEBUG'
    else:
        traceback = False
        loglevel = 'INFO'

    discard_all()

    worker.run(concurrency=CONF.num_workers,
               traceback=traceback, loglevel=loglevel)
