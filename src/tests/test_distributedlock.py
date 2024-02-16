#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 1/30/2024

@author: mlinvill
"""

import pytest

from multiprocessing import Value
from distributed.lock import DistributedLock, statedesc, InvalidPeerArgumentError, InvalidHostURIError


class TestDistributedLock():

    @pytest.fixture
    def test_instantiate(self):
        mynode = "127.0.0.1:8100"
        peerlist = ["127.0.0.1:8101", "127.0.0.1:8102", "127.0.0.1:8103"]
        leader_state = Value("i", 0, lock=True)

        self.distributedlock = DistributedLock(mynode, peerlist, leader_state)

    def test_instance(self):
        assert isinstance(self.distributedlock, DistributedLock)

    def test_attribute_syncobj(self):
        assert hasattr(self.distributedlock, "syncobj")

    def test_attribute_myip(self):
        assert hasattr(self.distributedlock, "myip")

    def test_attribute_lockmanager(self):
        assert hasattr(self.distributedlock, "lockmanager")

    def test_attribute_peers(self):
        assert isinstance(self.distributedlock.peers, "List")



    def test_run(self):
        #
        # The problem here is that this will run and communicate over the network until shutdown.
        #
        self.distributedlock.run()