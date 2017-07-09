#
# Author: Salo Shp <SaloShp@Gmail.Com>
#

import os
import sys
import argparse
import logging
import yaml
import json
from datetime import datetime
from elasticsearch import Elasticsearch

class YamlConfiguration(dict):
  pass

def init_logging():
  ''' Initialize logging configuration '''
  logging.basicConfig(format='%(asctime)-15s %(levelname).5s %(process)d [%(module)s] [%(funcName)s:%(lineno)d] - %(message)s')

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
  parser.add_argument('--interval', action='store', type=int, dest='interval', default=30,
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
    setattr(cfg, key, ymlcfg[key])
  for key in vars(args).keys():
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

