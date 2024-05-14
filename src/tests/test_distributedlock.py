#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 1/30/2024

@author: mlinvill
"""

import sys
from multiprocessing import Value
import threading
from ..distributedlock.distributed.lock import DistributedLock, statedesc, InvalidPeerArgumentError, InvalidHostURIError
from ..distributedlock.distributed.disco import Disco, PeerList

WATCHDOG_TIMEOUT = 60   # seconds


class TestDistributedLock:

    @classmethod
    def setup_class(cls):
        mynode = "127.0.0.1:8100"
        peerlist = ["127.0.0.1:8101", "127.0.0.1:8102", "127.0.0.1:8103"]
        leader_state = Value("i", 0, lock=True)

        cls.distributedlock = DistributedLock(mynode, peerlist, lockid=None, leader=leader_state)
        cls.distributedlock.run()

    @classmethod
    def teardown_class(cls):
        cls.distributedlock.shutdown()
        # Is this necessary/desirable?
        del cls.distributedlock

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
    for thread in threading.enumerate():
        thread.join(timeout=1)

    # raise(TimeoutError)
    sys.exit(1)


class TestDisco:
    """
    How deep? Test sending, receiving messages?
    """
    @classmethod
    def setup_class(cls):
        broker = "kafka.scimma.org"
        read_topic = "snews.testing"
        write_topic = "snews.testing"

        watchdog = threading.Timer(WATCHDOG_TIMEOUT, watchdog_timeout)
        watchdog.daemon = True
        watchdog.start()

        with Disco(broker=broker, read_topic=read_topic, write_topic=write_topic) as disco:
            # XXX - Check state here, no print
            print(f"Peers: {disco.get_peerlist()}")

        watchdog.cancel()

    @classmethod
    def teardown_class(cls):
        cls.disco.end()

        del cls.disco


class TestPeerList:

    @classmethod
    def setup_class(cls):
        cls.length = 0
        cls.peerlist = PeerList()

    def test_instance(self):
        assert isinstance(self.peerlist, PeerList)

    def test_add(self):
        self.peerlist.add_peer('127.0.0.1')

        assert (len(self.peerlist) == 1)

    def test_add_another(self):
        self.peerlist.add_peer('192.168.0.1')

        assert (len(self.peerlist) == 2)

    def test_remove_one(self):
        self.peerlist.remove_peer('127.0.0.1')

        assert (len(self.peerlist) == 1)

    def test_remove_another(self):
        self.peerlist.remove_peer('192.168.0.0.1')

        assert (len(self.peerlist) == 0)

    def test_add_part_two(self):
        self.peerlist.add_peer('10.0.0.1')
        self.peerlist.add_peer('10.0.0.2')
        self.peerlist.add_peer('10.0.0.3')

        assert (len(self.peerlist) == 3)

    def test_update_state_add(self):
        self.peerlist.set_state('10.0.0.5')

        assert (len(self.peerlist) == 4)

    def test_update_state_remove(self):
        self.peerlist.set_state('10.0.0.5')

        assert (len(self.peerlist) == 3)

    @classmethod
    def teardown_class(cls):
        del cls.peerlist
