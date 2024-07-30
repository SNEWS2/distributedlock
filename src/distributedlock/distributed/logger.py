#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on @date 

from https://python-tutorial.dev/201/tutorial/logging.html

@author: mlinvill
"""

import os
from pathlib import Path
from socket import gethostname
from logging import (
    getLogger,
    NullHandler,
    Formatter,
    FileHandler,
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    CRITICAL,
)

HOST = gethostname()
#handler = StreamHandler()


snewslog = os.getenv('SNEWSLOG')
if snewslog:
    log_file = Path(snewslog) / f"{HOST}.distributed_lock.log"
else:
    log_file = "distributed_lock.log"

fh = FileHandler(log_file)

formatter = Formatter(f'%(asctime)s {HOST} %(levelname)s [%(name)s] %(message)s',
                      datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)


# NOTE: NullHandler for library-only use
logger = getLogger('distributed_lock')
logger.addHandler(NullHandler())


levels = {'debug': DEBUG, 'info': INFO, 'warning': WARNING,
          'error': ERROR, 'critical': CRITICAL}


def initialize_logging(level: str) -> None:
    """Initialize the top-level logger with the stream handler and a `level`."""
    if fh not in logger.handlers:
#        logger.addHandler(handler)
        logger.addHandler(fh)
        logger.setLevel(levels.get(level))
