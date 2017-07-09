#
# Author: Salo Shp <SaloShp@Gmail.Com>
#

from __future__ import absolute_import

VERSION = (1, 0, 0, 0)
__version__ = VERSION
__versionstr__ = '.'.join(map(str, VERSION))

from .utils import *

args = parse_cli_args()
logger = init_logging()
cfg = init_config(args)

if cfg.debug:
  logger.setLevel(logging.DEBUG)

es = init_elastic(cfg)

def signal_handler(signal, frame):
  logger.debug("Caught signal '{}'".format(signal))
  if signal == 2:
    logger.debug("Aborting execution")
    sys.exit(0)
  else:
    es.indices.refresh()
    logger.info("Logging stats: {}".format(es.count(index='logs-*')))

###
logger.info("Initiated...")

