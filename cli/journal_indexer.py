#
# Author: Salo Shp <SaloShp@Gmail.Com>
#

import logging
import os
import threading
import queue
import time
from datetime import datetime
import os
import re
from elasticsearch import Elasticsearch
from elasticsearch import helpers

from pathlib import Path
import json
import select
from systemd import journal
from . import utils

logger = logging.getLogger('geeft')

class JournalIndexTask(dict):
  pass

class JournalIndexer():

  def __init__(self, es, cfg, hostplatform):
    self.queueLock = threading.Lock()
    self.taskQueue = queue.Queue()
    self.forks = cfg.forks

    today = datetime.now().strftime('%Y.%m.%d')
    self.index_name = 'logs_{}_{}'.format(today, hostplatform['hwuuid'])

    self.es = es
    self.hostplatform = hostplatform

    self.jrnl = journal.Reader()
    self.jrnl.this_boot()
    self.jrnl.log_level(journal.LOG_DEBUG)

    cursorpath = Path("/var/cache/geeft_systemd.cursor")
    if cursorpath.is_file():
      with open(cursorpath, 'r') as cursorfile:
        self.jrnl.seek_cursor(cursorfile.readline())
    else:
      self.jrnl.seek_head()

    p = select.poll()
    p.register(self.jrnl, self.jrnl.get_events())
    p.poll()
    self.jrnl.get_next()

  def index(self):
    logger.debug("Indexing journal")

    thread = JournalIndexThread(self.es, self.index_name, self.hostplatform, self.jrnl)
    thread.start()
    logger.debug("Finished indexing journal")

class JournalIndexThread(threading.Thread):

  def __init__(self, es, index, hostplatform, jrnl):
    threading.Thread.__init__(self)
    self.es = es
    self.index = index
    self.hostplatform = hostplatform
    self.jrnl = jrnl

  def run(self):
    logger.debug("Starting thread '{}'".format(self.name))

    while True:
      cursor = None
      actions = []
      for entry in self.jrnl:
        action, cursor = self._journal_entry_to_action(entry)
        actions.append(action)

      if len(actions) > 0:
        for success, info in helpers.parallel_bulk(self.es, actions, thread_count=2, index=self.index):
          if not success:
            logger.error('Failed: {}'.format(info[0]))
            return False

        self.es.indices.refresh()
        count = self.es.count(index=self.index)
        logger.debug("Items counted on index '{}': {}".format(self.index, count['count']))

        cursorpath = Path("/var/cache/geeft_systemd.cursor")
        with open(cursorpath, 'w') as cursorfile:
          cursorfile.write(cursor)
      time.sleep(1)

    logger.debug("Exiting thread '{}'".format(self.name))

  def _journal_entry_to_action(self, entry):
#    logger.debug(json.dumps(entry, indent=4, sort_keys=True, default=str, ensure_ascii=False))
    logger.debug("MSG - '{}'".format(entry['MESSAGE']))

    cursor = str(entry['__CURSOR'])
    service = entry['SYSLOG_IDENTIFIER']
    kvtag = {
      "hostuuid": self.hostplatform['hwuuid'],
      "hostplatform": self.hostplatform['platform'],
      "hostname": str(entry['_HOSTNAME']),
      "service": service,
      "priority": str(entry['PRIORITY']),
      "boot_id": entry['_BOOT_ID']
    }

    try:
      extracted_kvtags = utils._extract_kvtags(str(entry['MESSAGE']))
      kvtag.update(extracted_kvtags)
    except Exception as e:
      logger.exception(e)

    action = {
      "_type": "logs",
      "_source": {
        "data": str(entry['MESSAGE']),
        "@rotated": datetime.now(),
        "@created": entry['__REALTIME_TIMESTAMP'],
        "@indexed": datetime.now(),
        "host": str(entry['_HOSTNAME']),
        "tags": [service],
        "linenum": int(cursor.split(';')[1].split('=')[1], 16),
        "kvtag": kvtag
      }
    }
    return action, cursor

