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

]

CONF = cfg.CONF
CONF.register_cli_opts(cli_opts)
LOG = logging.getLogger(__name__)


def setup():
    CONF(sys.argv[1:])
    log_level = logging.DEBUG if CONF.debug else logging.INFO
    if hasattr(CONF, 'device_type'):
        log_format = ('%(asctime)s|[%(levelname)s] #%(process)d'
                      ' (%(name)s) <' + CONF.device_type + '> %(message)s')
    else:
        log_format = ('%(asctime)s|[%(levelname)s] #%(process)d'
                      ' (%(name)s) %(message)s')
    logging.basicConfig(level=log_level,
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S')


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


def do_collect():
    setup()
    hosts, metrics = reload_task()

    messages = []
    for metric in metrics.values():
        host = hosts[metric['host']]
        var = netsnmp.Varbind(metric['oid'])
        res = netsnmp.snmpget(var, Version=int(host.get('version', 1)),
                              DestHost=host.get('host'),
                              Community=host.get('community', 'public'))
        message = "%s %s %d" % (metric['metric'], res[0], time.time())
        messages.append(message)

    write_graphite(messages)

if __name__ == "__main__":
    do_collect()
