#
# Author: Salo Shp <SaloShp@Gmail.Com>
#

from __future__ import absolute_import

VERSION = (1, 0, 0, 0)
__version__ = VERSION
__versionstr__ = '.'.join(map(str, VERSION))

import platform
import sys
import subprocess
from .utils import *

args = parse_cli_args()
logger = init_logging()
cfg = init_config(args)
hwuuid_proc = subprocess.Popen('dmidecode -s system-uuid'.split(),stdout=subprocess.PIPE)
hwuuid = hwuuid_proc.stdout.readline().rstrip().decode("utf-8").lower()

if cfg.debug:
  logger.setLevel(logging.DEBUG)

hostplatform = dict()
hostplatform['sysversion'] = sys.version.split('\n')
hostplatform['dist'] = platform.dist()
hostplatform['linux_distribution'] = platform.linux_distribution()
hostplatform['system'] = platform.system()
hostplatform['machine'] = platform.machine()
hostplatform['platform'] = platform.platform()
hostplatform['uname'] = platform.uname()
hostplatform['version'] = platform.version()
hostplatform['mac_ver'] = platform.mac_ver()
hostplatform['hwuuid'] = hwuuid

es = init_elastic(cfg)

def signal_handler(signal, frame):
  logger.debug("Caught signal '{}'".format(signal))
  if signal == 2:
    logger.debug("Aborting execution")
    sys.exit(0)
  else:
    es.indices.refresh()
    logger.info("Logging stats: {}".format(es.count(index='logs_*')))

###
logger.info("Initiated %s" % hostplatform)

