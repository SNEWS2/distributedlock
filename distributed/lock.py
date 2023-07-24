#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 Created on 5/17/2023
 @author Mark Linvill

 PySyncObj distributed lock adapted from pysyncobj examples/docs.
"""

from typing import List

from time import sleep
from random import randint

import socket
from multiprocessing import Value

from rich.console import Console

from pysyncobj import SyncObj
from pysyncobj.batteries import ReplLockManager

statedesc = ["follower", "leader"]
STARTPORT = 8100

def getmyip() -> str:
    """
    Set up a socket to determine our ip address.

    :return: 'str'
        My ip as a string
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(0)
        try:
            sock.connect(("10.254.254.254", 1))
            myip = sock.getsockname()[0]
        except Exception:
            myip = "127.0.0.1"

    return myip


class DistributedLock:
    """
    Encapsulate distributed lock logic

    """
    def __init__(self, mynode: str, peerlist: List, leader: Value):
        self.autounlocktime = 35
        self.peers = peerlist or []
        self.lockmanager = None
        self.syncobj = None
        self.console = Console()
        self.myip = mynode
        self.leader = leader  # 1 for leader, 0 for follower

        if self.myip is None:
            self.myip = f"{getmyip()}:{STARTPORT}"

        assert self.myip is not False
        assert len(self.peers) != 0

    def run(self):
        """
        Run the pysyncobj raft protocol with the defined peer list.

        :return: None
        """

        self.lockmanager = ReplLockManager(autoUnlockTime=self.autounlocktime)
        self.syncobj = SyncObj(self.myip, self.peers, consumers=[self.lockmanager])

        while True:
            try:
                if self.lockmanager.tryAcquire(
                    "coincidenceLock", sync=True, timeout=randint(40, 60)
                ):
                    # we have the write lock
                    self.setleaderstate(True)
                    sleep(14)
                else:
                    self.setleaderstate(False)
                    sleep(randint(1, 5))

            except Exception as errmsg:
                print(f"Exception! {errmsg}")

                self.setleaderstate(False)
                self.lockmanager.release("coincidenceLock", sync=True)


    def setleaderstate(self, state: int):
        """
        attribute setter
        :param state:
        :return: None
        """
        self.leader.value = state

    def getleaderstate(self) -> int:
        """
        attribute getter
        :return: int: leader value
        """
        return self.leader.value
