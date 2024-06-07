#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

disco.py - Remote peer discovery over kafka topic via hop-client.

This should raise(?) events on peer changes!!!

Created on @date 

@author: mlinvill
"""

import sys
import copy
import socket
import json
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor
import queue
from hop import Stream
from hop.io import StartPosition

__all__ = ['Disco', 'PeerList',
           'DiscoTimeoutError', 'MissingArgumentError', 'UnknownActionError']

BROKER = "kafka.scimma.org"
READ_TOPIC = "snews.testing"
WRITE_TOPIC = "snews.testing"

""" We run discovery until we hear from (at least) this many peers.
"""
MIN_PEERS = 3
WATCHDOG_TIMEOUT = 60       # seconds
DISCO_STARTUP_DELAY = 30    # seconds

class MissingArgumentError(Exception):
    """ Missing Arguments Error
    """

class DiscoTimeoutError(Exception):
    """ Discovery protocol timeout error
    """

class UnknownActionError(Exception):
    """ Discovery protocol violation
    """

class Id(dict):
    """
    Inheriting from dict makes this json-serializable--required for hop messages

    Need to handle ports also.
    """
    def __init__(self):
        dict.__init__(self)
        self._myip: str

        """
        Set up a socket to determine our ip address.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(0)
            try:
                sock.connect(("10.254.254.254", 1))
                myip = sock.getsockname()[0]
            except Exception:
                myip = '127.0.0.1'

        self._myip = myip

    def getmyip(self):
        """ Return my ip address
        """
        return self._myip


class PeerList:
    """
    Object to hold our current list of discovered peers. Trigger events on status change.

    @property doesn't play nice with set(), so we use getters/setters.

    """
    def __init__(self):
        self._length = 0
        self._state = set()
        self._callbacks = []

    """ PUBLIC
    """
    def add_peer(self, peer):
        """ Add a peer by name to the list of known peers
        """
        self.set_state(peer)

    def remove_peer(self, peer):
        """ Remove a peer by name from the list of known peers
        """
        self.set_state(peer)

    def register_callback(self, callback):
        """ Register a function to call on peer state changes
        """
        self._callbacks.append(callback)

    def deregister_callback(self, callback):
        """ De-register a function previously registered to be called on peer state changes
        """
        self._callbacks.remove(callback)

    """ PRIVATE
    """
    def get_state(self):
        """ Return the state
        """
        return self._state

    def set_state(self, thing):
        """ Change the state
        """
        old_state = copy.deepcopy(self._state)

        if thing in self._state:
            # remove, if the peer is known
            self._state.discard(thing)
        else:
            # add
            self._state.add(thing)

        self._length = len(self._state)
        self._notify(old_state, self.get_state())

    def _notify(self, old_state, new_state):
        """ Call the functions that registered an interest in state changes
        """
        for callback in self._callbacks:
            callback(old_state, new_state)

    def __len__(self):
        """ Dunder method to return the number of known peers
        """
        return self._length


class Disco:
    """
        Interface to remote registration/status/discovery for distributed lock peers/network.

        TODO -
            This needs additional functionality to supervise known peers,
            to become aware of when they go away/drop off the network. Some kind of periodic
            ping/query/tickle thread. Cache?

    :return:
    """
    def __init__(self, *args, **kwargs):
        self._me = None
        self._auth = True
        self._topic = None
        self._broker = None
        self._read_topic = None
        self._write_topic = None
        self._stream_uri_r = None
        self._stream_uri_w = None
        self._stream_r = None
        self._stream_w = None
        self._in_disco = False
        self._peerlist = PeerList()
        self._id = None
        self._queue = None
        self._event = None

        """ setup broker, topic attributes
        """
        for k, v in kwargs.items():
            key = f"_{k}"
            self.__dict__[key] = v

        self._queue = queue.Queue(maxsize=15)
        self._event = threading.Event()

        if self._broker:
            if self._read_topic:
                if "kafka://" in self._broker.lower():
                    self._stream_uri_r = f"kafka://{self._broker}/{self._read_topic}"
                else:
                    self._stream_uri_r = f"kafka://{self._broker}/{self._read_topic}"

            if self._write_topic:
                if "kafka" in self._broker.lower():
                    self._stream_uri_w = f"kafka://{self._broker}/{self._write_topic}"
                else:
                    self._stream_uri_w = f"kafka://{self._broker}/{self._write_topic}"
        else:
            raise MissingArgumentError

        with ThreadPoolExecutor(max_workers=2) as executor:
#            executor.submit(self.produce, self._queue, self._event)
            executor.submit(self.consume, self._queue, self._event)

        """ Determine my address """
        self._id = Id()

    def __enter__(self):
        if self._stream_uri_r:
            self._stream_r = Stream(until_eos=True,
                                    auth=self._auth,
                                    start_at=StartPosition.EARLIEST).open(self._stream_uri_r, 'r')

        if self._stream_uri_w:
            self._stream_w = Stream(until_eos=True, auth=self._auth).open(self._stream_uri_w, 'w')

        time.sleep(DISCO_STARTUP_DELAY)
        self.discovery()

        return self

    def __exit__(self, *_):
        self._stream_r.close()
        self._stream_w.close()
        self._event.set()

    def _send(self, msg):
        """ Encapsulate the logic/method of actually writing
        """
        if len(msg) > 1 and self._stream_w:
            return self._stream_w.write(msg)

        return False

    def _recv(self):
        """ Encapsulate the logic/method of actually reading
        """
        if self._stream_r:
            return self._stream_r

        return False

    def poll(self):
        """ Main logic for the protocol, wait for messages, register peers, end when we have enough
        """
        endit = False
        time.sleep(2)

        while not endit:
            for message in self.consume(self._queue, self._event):
                msg = json.loads(message.content)

                if self._id.getmyip() == msg['source']:
                    print("skipping message from myself")
                    continue

                if 'DISCO' in msg['action']:
                    self._in_disco = True
                    self.reply()
                elif 'REPLY' in msg['action']:
                    self._peerlist.add_peer(msg['REPLY'])
                    if len(self._peerlist) >= MIN_PEERS:
                        self.end()
                elif 'END' in msg['action']:
                    endit = True
                else:
                    raise UnknownActionError

            if endit:
                break

            time.sleep(2 + random.randint(0, 4))

    def discovery(self):
        """ Launch the discovery protocol. Find peers.
        """
        if not self._in_disco:
            self._in_disco = True
            self._send(json.dumps("{'action' : 'DISCO', " +
                                  " 'source' : " + json.dumps(self._id.getmyip()).encode("utf-8") +
                                  "}"))

        time.sleep(random.randint(1, 3))

        self.poll()

    def reply(self):
        """ Reply to a discovery request
        """
        self._send(json.dumps("{'REPLY' : " + json.dumps(self._id.getmyip()).encode("utf-8") +
                              ", 'source' : " + json.dumps(self._id.getmyip()).encode("utf-8") +
                              "}"))

    def end(self):
        """ Send the 'end' discovery protocol action
        """
        self._send(json.dumps("{'action' : 'END', " +
                              " 'source' : " + json.dumps(self._id.getmyip()).encode("utf-8") +
                              "}"))

    @staticmethod
    def produce(que: queue.Queue, event: threading.Event, msg: str):
        """ Put msg in the work queue
        """
        while not event.is_set():
            que.put(msg)

    def consume(self, que: queue.Queue, event: threading.Event):
        """ Buffers!
            TODO -
            XXX - This should be profiled to see if buffering is useful or needed here.
            XXX - There is also certainly a better algorithm to drain the queue to some threshold
                  if it's full before putting anything else onto it.
        """
        if que.full():
            time.sleep(10)

        while not event.is_set() and not que.empty():
            try:
                if not que.full():
                    for msg in self._stream_r:
                        que.put(msg)

                message = que.get()
                yield message

            except que.full():
                time.sleep(10)
                message = que.get()
                yield message

    def get_peerlist(self):
        """ Return the list of peers
        """
        return self._peerlist


def watchdog_timeout():
    """ Watchdog timer implementation for the discovery protocol
    """
    print("Watchdog time-out!")
    """ This hangs in the socket read, doesn't actually exit/end.
    """
    for thrd in threading.enumerate():
        thrd.join(timeout=1)

    # raise(DiscoTimeoutError)
    sys.exit(1)


if __name__ == "__main__":

    watchdog = threading.Timer(WATCHDOG_TIMEOUT, watchdog_timeout)
    watchdog.daemon = True
    watchdog.start()

    with Disco(broker=BROKER, read_topic=READ_TOPIC, write_topic=WRITE_TOPIC) as disco:
        print(f"Peers: {disco.get_peerlist()}")

    watchdog.cancel()
