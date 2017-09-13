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
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import uuid

logger = logging.getLogger('geeft')

class FileIndexTask(dict):
  pass

class FileIndexer():

  def __init__(self, es, cfg):
    self.queueLock = threading.Lock()
    self.taskQueue = queue.Queue()
    self.forks = cfg.forks

    self.spool_dir = cfg.spool_dir
    self.spool_completed_suffix = cfg.spool_completed_suffix
    self.spool_completed_action = cfg.spool_completed_action
    self.parsing = cfg.parsing

    today = datetime.now().strftime('%Y.%m.%d')
    self.index_name = 'logs-' + today
    self.use_temp_index = cfg.use_temp_index

    self.es = es

  def index_spool_dir(self):
    logger.debug("Parsing spool dir: %s" % self.spool_dir)
    oswalk = os.walk(self.spool_dir)
    for dirname, dirs, files in oswalk:
      for file in files:
        if file.endswith(self.spool_completed_suffix):
          continue
        task = FileIndexTask()
        task.ts = [datetime.now()]
        task.filename = file
        task.service = file.split('.')[0]
        task.parsing = None
        if task.service in self.parsing:
          task.parsing = self.parsing[task.service]
        task.dirname = dirname
        task.file_path = os.path.join(dirname, file)
        task.index_name = self.index_name
        task.use_temp_index = self.use_temp_index
        task.temp_index_name = 'temp-' + str(uuid.uuid4())
        task.reindex_query = { 'source': { 'index': task.temp_index_name }, 'dest': { 'index': task.index_name } }
        task.spool_completed_action = self.spool_completed_action
        task.spool_completed_suffix = self.spool_completed_suffix

        self.taskQueue.put(task)

    if self.taskQueue.qsize() > 0:
        logger.info("Indexing {} files".format(self.taskQueue.qsize()))

    num_forks = min(self.forks, self.taskQueue.qsize())
    if num_forks < 1:
      return

    logger.debug("Starting {} worker threads".format( num_forks ))
    threads = []
    for i in range( num_forks ):
      thread = FileIndexThread(self.es, self.taskQueue, self.queueLock)
      thread.start()
      threads.append(thread)

    # block until all tasks are done
    self.taskQueue.join()

    # stop workers
    for t in threads:
      t.join()

class FileIndexThread(threading.Thread):

  def __init__(self, es, taskQueue, queueLock):
    threading.Thread.__init__(self)
    self.es = es
    self.taskQueue = taskQueue
    self.queueLock = queueLock

  def run(self):
    logger.debug("Starting thread '{}'".format(self.name))
    while not self.taskQueue.empty():
      self.queueLock.acquire()
      if not self.taskQueue.empty():
        task = self.taskQueue.get()
      self.queueLock.release()

      task.ts.append(datetime.now())
      res = self._handle_task(task)
      if res:
        task.ts.append(datetime.now())
        logger.info("Task '{}' ({}) completed in '{}' '{}'".format(task.filename, task.count['count'], task.ts[2] - task.ts[0], task.ts[2] - task.ts[1]))
      else:
        logger.error("Error performing task '{}'".format(task.filename))

      self.queueLock.acquire()
      self.taskQueue.task_done()
      self.queueLock.release()

    logger.debug("Exiting thread '{}'".format(self.name))

  def _generate_bulk_elastic_actions_from_file(self, file_path, service, parsing):
    logger.debug("Generating bulk actions from file '{}'".format(file_path))
    date = datetime.now()
    last_date = datetime.now()
  #  actions = []
    file = open(file_path,'r')
    for line in file.readlines():
  #    if line == '' or line == '\n':
  #      continue
      if parsing is not None:
        regex = parsing['regex']
        date_format = parsing['date_format']
        try:
          extracted_date = re.search(regex, line).group(0)
          date = datetime.strptime(extracted_date, date_format)
          last_date = date
        except AttributeError as e:
#          print(e)
          pass
  #        raise

      action = {
        "_type": "logs",
        "_source": {
          "data": line,
          "@created": last_date,
          "@indexed": datetime.now(),
          "host": os.uname()[1],
          "tags": [service],
          "kvtag": {
            "host": os.uname()[1],
            "service": service
          }
        }
      }
#      print(action)
      yield action
  #    actions.append(action)
  #  return actions

  def _handle_task(self, task):
    if task.use_temp_index:
      self.es.indices.create(task.temp_index_name, request_timeout=30)
      logger.debug("Created temporary index '{}'".format(task.filename))
      index = task.temp_index_name
    else:
      index = task.index_name

    logger.debug("Indexing file '{}' to '{}'".format(task.file_path, index))
    file = open(task.file_path, 'r')
    try:
      for success, info in helpers.parallel_bulk(self.es, self._generate_bulk_elastic_actions_from_file(task.file_path, task.service, task.parsing), thread_count=2, index=index):
        if not success:
          logger.error('Failed: {}'.format(info[0]))
          return False
      self.es.indices.refresh()
      count = self.es.count(index=index)
      logger.debug("Items counted on index '{}': {}".format(index, count['count']))

      msg = "Indexed file '{}'".format(task.filename)
      logger.debug(msg)
      task.count = count

      if task.use_temp_index:
        res = self.es.reindex(task.reindex_query, request_timeout=30)
        msg = "Reindexed file '{}' data from '{}' to '{}'".format(task.file_path, task.temp_index_name, task.index_name)
        logger.debug(msg)
      else:
        res = {'failures': []}

      if res['failures'] == []:
        if task.spool_completed_action == 'remove':
          logger.info("Deleting file '{}'".format(task.file_path) )
          os.remove(task.file_path)
        elif task.spool_completed_action == 'rename':
          logger.info("Renaming file '{}' to {}".format(task.file_path, task.spool_completed_suffix) )
          os.rename(task.file_path, task.file_path + task.spool_completed_suffix)

      msg = "Completed file {}".format(task.file_path)
      logger.debug(msg)

    except Exception as e:
      logger.error(e)
      return False

    file.close()
    return True
