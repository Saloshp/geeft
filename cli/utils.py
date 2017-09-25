#
# Author: Salo Shp <SaloShp@Gmail.Com>
#

import os
import sys
import argparse
import logging
import yaml
import json
import re
from datetime import datetime
from elasticsearch import Elasticsearch

class YamlConfiguration(dict):
  pass

def init_logging():
  ''' Initialize logging configuration '''
  logging.basicConfig(filename='/var/log/geeft.log', format='%(asctime)-15s %(levelname).5s %(process)d [%(module)s] [%(funcName)s:%(lineno)d] - %(message)s')

  logger = logging.getLogger('geeft')
  logger.setLevel(logging.INFO)
  return logger

def parse_cli_args():
  ''' Command line argument processing '''

  parser = argparse.ArgumentParser(
    description='Index files to Elasticsearch',
    usage='%(prog)s --debug --iterations 1',
    epilog='2017 (C) Salo Shp <SaloShp@Gmail.Com>'
  )
  parser.add_argument('--debug', action='store_true', default=False,
    help='Debug mode (default: False)')
  parser.add_argument('--forks', action='store', type=int, dest='forks', default=os.cpu_count(),
    help='Specify number of worker threads (default: number of cores)')
  parser.add_argument('--iterations', action='store', type=int, dest='iterations', default=1,
    help='Specify number of iterations, 0 for unlimited (default: 1)')
# TODO default=30
  parser.add_argument('--interval', action='store', type=int, dest='timer_interval',
    help='Set timer interval in seconds (default: 30 seconds)')
  args = parser.parse_args()

  return args

def init_config(args):
  ''' Initialize YAML configuration file '''
  with open('geeft.yml', 'r') as ymlfile:
      ymlcfg = yaml.load(ymlfile)

  cfg = YamlConfiguration()

#  query_file_path = 'agg_all.query'

#  query_file = open(query_file_path, 'r')
#  query_json = json.load(query_file)

  for key in ymlcfg.keys():
    if ymlcfg[key] is not None:
      setattr(cfg, key, ymlcfg[key])
  for key in vars(args).keys():
    if vars(args)[key] is not None:
      setattr(cfg, key, vars(args)[key])

  if args.iterations == 0:
    cfg.unlimited_iterations = True
  else:
    cfg.unlimited_iterations = False

  return cfg

def init_elastic(cfg):
  elastic_hosts = cfg.elastic_hosts
  es = Elasticsearch(elastic_hosts, maxsize=cfg.forks*2+1)

  es.indices.refresh(index='telegraf-*')
  es.indices.refresh(index='logs*')

  return es
  #res = es.indices.delete('logs*')
  #es.indices.refresh()

def _generate_bulk_elastic_actions_from_file(rotatetime, file_path, service, logname, parsing, hostplatform):
  logger = logging.getLogger('geeft')
  logger.debug("Generating bulk actions from file '{}'".format(file_path))
  now = datetime.now()
  date = now
  last_date = now
#  actions = []
  file = open(file_path, 'r', encoding='utf8')
  file_index = 0
  for line in file.readlines():
    file_index += 1
#    if line == '' or line == '\n':
#      continue
    if parsing is not None:
      regex = parsing['regex']
      date_format = parsing['date_format']
#      logger.debug('Date format: {}'.format(date_format))
      try:
#          logger.debug('Regex: {}'.format(regex))
        extracted_date = re.search(regex, line).group(0)
        date = datetime.strptime(extracted_date, date_format)
        date = date.replace(year=now.year)
        last_date = date
      except AttributeError as e:
#        logger.exception(e)
        pass
#        raise

    kvtag = {
      "hostuuid": hostplatform['hwuuid'],
      "hostplatform": hostplatform['platform'],
      "hostname": os.uname()[1],
      "service": service
    }

    extracted_kvtags = _extract_kvtags(line)
    kvtag.update(extracted_kvtags)

    action = {
      "_type": "logs",
      "_source": {
        "data": line,
        "@rotated": datetime.utcfromtimestamp(rotatetime).replace(microsecond=1),
        "@created": last_date,
        "@indexed": datetime.now(),
        "host": os.uname()[1],
        "tags": [service, logname],
        "linenum": file_index,
        "kvtag": kvtag
      }
    }

#    logger.debug(action)
    yield action
#    actions.append(action)
#  return actions

def _extract_kvtags(data):
    kvtag = {}

    extracted_ips = set()
    try:
      for ip in re.findall(r'((\d{1,3}\.){3}(\d{1,3}))(:[0-9]{5})?', data):
          extracted_ips.add(ip[0])
      if len(extracted_ips) > 0:
          kvtag['ips'] = list(extracted_ips)
    except AttributeError as e:
      pass

    extracted_uuids = set()
    try:
      for uuid in re.findall(r'[0-9a-fA-F-]{36}', data):
          extracted_uuids.add(uuid)
      if len(extracted_uuids) > 0:
          kvtag['uuids'] = list(extracted_uuids)
    except AttributeError as e:
      pass

    extracted_paths = set()
    try:
      for path in re.findall(r'(((http)(s)?:/)?(/([a-zA-Z0-9-._]+)(/[a-zA-Z0-9-._]+)+))', data):
          extracted_paths.add(path[0])
      if len(extracted_paths) > 0:
          kvtag['paths'] = list(extracted_paths)
    except AttributeError as e:
      pass

    extracted_java_exceptions = set()
    try:
      for exception in re.findall(r'(.*Exception).*', data):
          extracted_java_exceptions.add(exception[0])
      if len(extracted_java_exceptions) > 0:
          kvtag['exception'] = list(extracted_java_exceptions)
    except AttributeError as e:
      pass

    extracted_levels = set()
    try:
      for level in re.findall(r'trace|debug|notice|info|warning|warn|fatal|failure|fail|exception|severe|error', data, re.IGNORECASE):
          extracted_levels.add(level)
      if len(extracted_levels) > 0:
          kvtag['level'] = list(extracted_levels)
    except AttributeError as e:
      pass

    return kvtag
