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
import uuid
from multiprocessing import Value
from pysyncobj import SyncObj
from pysyncobj.batteries import ReplLockManager

from .logger import getLogger, initialize_logging
log = getLogger("distributed_lock")

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

    log.debug(f"getmyip() my ip is {myip}")
    return myip

class InvalidPeerArgumentError(Exception):
    pass

class InvalidHostURIError(Exception):
    pass

class InvalidLockIdError(Exception):
    pass


class DistributedLock:
    """
    Encapsulate distributed lock logic
    """
    def __init__(self, mynode: str, peerlist: List, lockid: str, leader: Value):
        self._run = True
        self.autounlocktime = 13
        self.peers = peerlist or []
        self.lockmanager = None
        self.syncobj = None
        self.myip = mynode

        if not lockid or lockid is None:
            print("Warning! You should provide a lockid! Hint: try genlockid()")
            lockid = self.genlockid()

        self.lockid = lockid
        self.leader = leader  # multiprocessing Value type, leader.value: 1 for leader, 0 for follower

        if self.myip is None:
            self.myip = f"{getmyip()}:{STARTPORT}"

        if len(self.peers) < 3:
            raise InvalidPeerArgumentError

        log.debug(f"DistributedLock.__init__(): myip {self.myip} peers {self.peers}")

    def genlockid(self) -> str:
        return str(uuid.uuid1().hex)

    def run(self):
        """
        Run the pysyncobj raft protocol with the defined peer list.

        :return: None
        """

        self.lockmanager = ReplLockManager(autoUnlockTime=self.autounlocktime)
        self.syncobj = SyncObj(self.myip, self.peers, consumers=[self.lockmanager])

        while self._run:
            try:
                if self.lockmanager.tryAcquire(
                    self.lockid, sync=True, timeout=randint(30, 60)
                ):
                    log.debug(f"{self.myip} I have the lock!")
                    # we have the write lock
                    self.setleaderstate(True)
                    sleep(10)
                else:
                    self.setleaderstate(False)
                    sleep(randint(1, 15))

            except Exception as errmsg:
                log.error(f"Exception trying to aquire lock.")
                self.stop()
                raise

        self.stop()

    def stop(self) -> None:
        log.warn(f"{self.myip} ...Stopping.")

        self.setleaderstate(False)
        self.lockmanager.release(self.lockid, sync=True)

    def shutdown(self) -> None:
        self._run = False

    def setleaderstate(self, state: int) -> None:
        """
        attribute setter
        :param state:
        :return: None
        """
        with self.leader.get_lock():
            self.leader.value = state

    def getleaderstate(self) -> int:
        """
        attribute getter
        :return: int: leader value
        """
        return self.leader.value
