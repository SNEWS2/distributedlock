#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

disco.py - Remote peer discovery over kafka topic via hop-client.

This should raise(?) events on peer changes!!!

Created on @date 

@author: mlinvill
"""

import sys
import socket
import json
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, thread
import queue
from hop import Stream
from hop.io import StartPosition


broker = "kafka.scimma.org"
read_topic = "snews.testing"
write_topic = "snews.testing"

""" We run discovery until we hear from (at least) this many peers.
"""
MIN_PEERS = 3
WATCHDOG_TIMEOUT = 60   # seconds


class MissingArgumentError(Exception):
    pass


class TimeoutError(Exception):
    pass


class UnknownActionError(Exception):
    pass


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
                myip = "127.0.0.1"

        self._myip = myip

    def getmyip(self):
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
        self.set_state(peer)

    def remove_peer(self, peer):
        self.set_state(peer)

    def register_callback(self, callback):
        self._callbacks.append(callback)

    """ PRIVATE
    """
    def get_state(self):
        return self._state

    def set_state(self, thing):
        old_state = self._state

        if thing in self._state:
            # remove, if the peer is known
            self._state.discard(thing)
        else:
            # add
            self._state.add(thing)

        self._length = len(self._state)
        self._notify(old_state, self.get_state())

    def _notify(self, old_state, new_state):
        for callback in self._callbacks:
            callback(old_state, new_state)

    def __len__(self):
        return self._length


class Disco:
    """
        Interface to remote registration/status/discovery for distributed lock peers/network.

        TODO -
            This needs additional functionality to supervise known peers,
            to become aware of when they go away/drop off the network. Some kind of periodic
            ping/query/tickle thread.

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
        self._Id = None
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
            executor.submit(self.produce, self._queue, self._event)
            executor.submit(self.consume, self._queue, self._event)
            thread._threads_queues.clear()           # dirty hack to avoid waiting on long-running
                                                     # threads to exit

        """ Determine my address """
        self._Id = Id()

    def __enter__(self):
        if self._stream_uri_r:
            self._stream_r = Stream(until_eos=True,
                                    auth=self._auth,
                                    start_at=StartPosition.EARLIEST).open(self._stream_uri_r, 'r')

        if self._stream_uri_w:
            self._stream_w = Stream(until_eos=True, auth=self._auth).open(self._stream_uri_w, 'w')

        time.sleep(2)
        self.discovery()

        return self

    def __exit__(self):
        self._stream_r.close()
        self._stream_w.close()
        self._event.set()

    def _send(self, msg):
        """ Encapsulate the logic/method of actually writing
        """
        if len(msg) > 1 and self._stream_w:
            return self._stream_w.write(msg)
        else:
            return False

    def _recv(self):
        """ Encapsulate the logic/method of actually reading
        """
        if self._stream_r:
            return self._stream_r

    def poll(self):
        endit = False
        time.sleep(2)

        while not endit:
            for message in self.consume(self._queue, self._event):
                msg = json.loads(message.content)

                if self._Id.getmyip() == msg['source']:
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
        """ Ask who is out there
        """
        if not self._in_disco:
            self._in_disco = True
            self._send(json.dumps("{'action' : 'DISCO', " +
                                  " 'source' : '" + f"{json.dumps(self._Id.getmyip())}" +
                                  "'}"))

        time.sleep(random.randint(1, 3))

        self.poll()

    def reply(self):
        """ Reply to a discovery request
        """
        self._send(json.dumps("{'REPLY' : " + f"{json.dumps(self._Id.getmyip())}" +
                              ", 'source' : '" + f"{json.dumps(self._Id.getmyip())}" +
                              "'}"))

    def end(self):
        self._send(json.dumps("{'action' : 'END', " +
                              " 'source' : '" + f"{json.dumps(self._Id.getmyip())}" +
                              "'}"))

    @staticmethod
    def produce(que, event, msg):
        while not event.is_set():
            que.put(msg)

    def consume(self, que, event):
        """ Buffers!
            XXX - This should be profiled to see if buffering is useful or needed here.
            XXX - There is also certainly a better algorithm to drain the queue to some threshold
                  if it's full before putting anything else onto it.
        """
        if que.full():
            time.sleep(10)

        while not event.is_set() and que.not_empty():
            try:
                if que.not_full():
                    for msg in self._stream_r:
                        self._queue.put(msg)

                message = que.get()
                yield message

            except queue.Full:
                time.sleep(10)
                message = que.get()
                yield message

    def get_peerlist(self):
        return self._peerlist


def watchdog_timeout():
    print("Watchdog time-out!")
    """ This hangs in the socket read, doesn't actually exit/end.
    """
    for thrd in threading.enumerate():
        thrd.join(timeout=1)

    # raise(TimeoutError)
    sys.exit(1)


if __name__ == "__main__":

    watchdog = threading.Timer(WATCHDOG_TIMEOUT, watchdog_timeout)
    watchdog.daemon = True
    watchdog.start()

    with Disco(broker=broker, read_topic=read_topic, write_topic=write_topic) as disco:
        print(f"Peers: {disco.get_peerlist()}")

    watchdog.cancel()
