#!/usr/bin/python3
#
# Author: Salo Shp <SaloShp@Gmail.Com>
#

import time
import signal

from cli import *
from cli.file_indexer import FileIndexer
from cli.journal_indexer import JournalIndexer

logger = logging.getLogger('geeft')

def main():
  logger.debug('main')
  journalIndexer = JournalIndexer(es, cfg, hostplatform)
  journalIndexer.index()

  while cfg.iterations > 0 or cfg.unlimited_iterations:
    logger.debug('Iterating..')
    cfg.iterations -= 1

    fileIndexer = FileIndexer(es, cfg, hostplatform)
    fileIndexer.index()

    res = es.indices.delete('temp*')

    if cfg.iterations > 0 or cfg.unlimited_iterations:
      logger.debug('Sleeping for {}'.format(cfg.timer_interval))
      time.sleep(cfg.timer_interval)

    logger.debug('Done iterating..')


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTSTP, signal_handler)
# TODO XXX
#    signal.signal(signal.SIGTERM, signal_handler)
    main()
