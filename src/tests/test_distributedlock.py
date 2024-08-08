#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 1/30/2024

@author: mlinvill
"""

import sys
from typing import List
import multiprocessing as mp
from multiprocessing import Value
import threading
from distributedlock.distributed.lock import DistributedLock, statedesc
from distributedlock.distributed.disco import Disco, PeerList

WATCHDOG_TIMEOUT = 60  # seconds


def cb_output(old, new):
    """Dummy callback function"""
    print(f"old: {old} / new: {new}")


def runlock(mynode: str, peerlist: List, leader_state: Value):
    """
    Create a DistributedLock instance and run it.

    :param mynode: str
    :param peerlist: List
    :param leader_state: Value
    :return: None
    """
    distributedlock = DistributedLock(mynode, peerlist, lockid=None, leader=leader_state)
    distributedlock.run()


class TestPeerList:
    """ Test the PeerList class """

    def setup_class(self):
        """Setup PeerList"""
        self.length = 0
        self.peerlist = PeerList()

    def test_instance(self):
        """Is it a PeerList"""
        assert isinstance(self.peerlist, PeerList)

    def test_add(self):
        """Test add function"""
        self.peerlist.add_peer("127.0.0.1")

        assert len(self.peerlist) == 1

    def test_add_another(self):
        """Test add function"""
        self.peerlist.add_peer("192.168.0.1")

        assert len(self.peerlist) == 2

    def test_remove_one(self):
        """Test remove"""
        self.peerlist.remove_peer("127.0.0.1")

        assert len(self.peerlist) == 1

    def test_callback(self):
        """Test callback"""
        self.peerlist.register_callback(cb_output)

    def test_remove_another(self):
        """Test remove"""
        self.peerlist.remove_peer("192.168.0.1")

        assert len(self.peerlist) == 0

    def test_add_part_two(self):
        """Test add another"""
        self.peerlist.add_peer("10.0.0.1")
        self.peerlist.add_peer("10.0.0.2")
        self.peerlist.add_peer("10.0.0.3")

        assert len(self.peerlist) == 3

    def test_remove_callback(self):
        """Test removing a callback"""
        self.peerlist.deregister_callback(cb_output)


    def teardown_class(self):
        """Cleanup"""
        del self.peerlist


class TestDisco:
    """
    How deep? Test sending, receiving messages?
    """

    @classmethod
    def setup_class(cls):
        cls.broker = "kafka.scimma.org"
        cls.read_topic = "snews.operations"
        cls.write_topic = "snews.operations"

        cls.watchdog = threading.Timer(WATCHDOG_TIMEOUT, watchdog_timeout)
        cls.watchdog.daemon = True
        cls.watchdog.start()

    def test_disco(self):
        with Disco(
            broker=self.broker, read_topic=self.read_topic, write_topic=self.write_topic
        ) as self.disco:
            print(f"Peers: {self.disco.get_peerlist()}")

        self.watchdog.cancel()

    @classmethod
    def teardown_class(cls):
        cls.watchdog.cancel()

        del cls.watchdog
        #del cls.disco


class TestDistributedLock:
    """ Test DistributedLock class """
    @classmethod
    def setup_class(cls):
        mynode = "127.0.0.1:8100"
        peerlist = ["127.0.0.1:8101", "127.0.0.1:8102", "127.0.0.1:8103"]
        leader_state = Value("i", 0, lock=True)

        # cls.distributedlock = DistributedLock(mynode, peerlist, lockid=None, leader=leader_state)
        # cls.distributedlock.run()

        cls.lock_process = mp.Process(
            target=runlock, args=(mynode, peerlist, leader_state)
        )
        cls.lock_process.start()

    #    @classmethod
    #    def teardown_class(cls):
    #        cls.lock_process.shutdown()
    #        cls.distributedlock.shutdown()
    #        # Is this necessary/desirable?
    #        del cls.distributedlock

    def test_instance(self):
        assert isinstance(self.distributedlock, DistributedLock)

    def test_attribute_syncobj(self):
        assert hasattr(self.distributedlock, "syncobj")

    def test_attribute_myip(self):
        assert hasattr(self.distributedlock, "myip")

    def test_attribute_lockmanager(self):
        assert hasattr(self.distributedlock, "lockmanager")

    def test_attribute_peers(self):
        assert isinstance(self.distributedlock.peers, list)


def watchdog_timeout():
    print("Watchdog time-out!")
    """ This hangs in the socket read, doesn't actually exit/end.
    """
#    for thread in threading.enumerate():
#        thread.join(timeout=1)

    # raise(TimeoutError)
    sys.exit(1)
