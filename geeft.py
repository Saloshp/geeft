#!/usr/bin/python3
#
# Author: Salo Shp <SaloShp@Gmail.Com>
#

import time
import signal

from cli import *
from cli.file_indexer import FileIndexer

def main():

  while cfg.iterations > 0 or cfg.unlimited_iterations:
    cfg.iterations -= 1
    fileIndexer = FileIndexer(es, cfg)
    fileIndexer.index_spool_dir()

    res = es.indices.delete('temp*')

    if cfg.iterations > 0 or cfg.unlimited_iterations:
      time.sleep(cfg.interval)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTSTP, signal_handler)
    main()
