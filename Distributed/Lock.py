#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 5/17/2023
# @author Mark Linvill
#
# PySyncObj distributed lock adapted from pysyncobj examples/docs.

from typing import List

from time import sleep
from random import randint
from rich.console import Console

from pysyncobj import SyncObj
from pysyncobj.batteries import ReplLockManager
from multiprocessing import Value

import socket

statedesc = ['follower', 'leader']


def getmyip() -> str:
    """
    Set up a socket to determine our ip address.

    :return: myip: My ip as a string
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(0)
        try:
            s.connect(('10.254.254.254', 1))
            myip = s.getsockname()[0]
        except Exception:
            myip = '127.0.0.1'

    return myip


class DistributedLock:
    def __init__(self, me: str, peers: List, leader: Value):
        self.autounlocktime = 15
        self.peers = peers or []
        self.lockmanager = None
        self.syncobj = None
        self.console = Console()
        self.myip = me
        self.port = 8100
        self.leader = leader    # 1 for leader, 0 for follower
        self.pysyncobj_version = SyncObj.getCodeVersion()

        if self.myip is None:
            self.myip = f"{getmyip()}:{self.port}"

        assert self.myip is not False
        assert len(self.peers)

    def run(self):
        """
        Run the pysyncobj raft protocol with the defined peer list.

        :return: None
        """

        self.lockmanager = ReplLockManager(autoUnlockTime=self.autounlocktime)
        self.syncobj = SyncObj(self.myip, self.peers, consumers=[self.lockmanager])

        while True:
            try:
                if self.lockmanager.tryAcquire('coincidenceLock', sync=True, timeout=randint(40, 60)):
                    # we have the write lock
                    self.setleaderstate(True)
                    sleep(14)
                else:
                    self.setleaderstate(False)
                    sleep(randint(1, 5))

            except Exception as e:
                print(f"Exception! {e}")
            finally:
                self.setleaderstate(False)
                self.lockmanager.release('coincidenceLock', sync=True)


    def setleaderstate(self, state: int):
        self.leader.value = state

    def getleaderstate(self) -> int:
        return self.leader.value