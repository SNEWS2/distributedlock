#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

disco.py - Remote peer discovery over kafka topic via hop-client.

This should raise(?) events on peer changes!!!

Created on @date 

@author: mlinvill
"""

# import sys
import copy
import socket
import json
import time
import random
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
import queue
from hop import Stream
# from hop.io import StartPosition
from .logger import getLogger

__all__ = ['Disco', 'PeerList',
           'BROKER', 'READ_TOPIC', 'WRITE_TOPIC',
           'DiscoTimeoutError', 'MissingArgumentError', 'UnknownActionError']

BROKER = "kafka.scimma.org"
READ_TOPIC = "snews.operations"
WRITE_TOPIC = "snews.operations"

""" We run discovery until we hear from (at least) this many peers.
"""
MIN_PEERS = 2
WATCHDOG_TIMEOUT = 60  # seconds
DISCO_STARTUP_DELAY = 30  # seconds

log = getLogger("distributed_lock")
log.setLevel(logging.DEBUG)


class NetworkError(Exception):
    """ Catch-all for network problems
    """


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
                """ This can throw OSError or TimeoutError, but we don't care. Any
                    exception results in not being able to determine our ip and fall-back 
                    to a default value.
                """
                sock.connect(("10.254.254.254", 1))
                myip = sock.getsockname()[0]
            except:
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

    # PUBLIC
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

    # PRIVATE
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
        """ Return the number of known peers
        """
        return self._length

    def __repr__(self):
        return self._state


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
        self._in_queue = None
        self._out_queue = None
        self._event = None
        self._endit = False

        """ setup broker, topic attributes
        """
        for k, v in kwargs.items():
            key = f"_{k}"
            self.__dict__[key] = v

        self._in_queue = queue.Queue(maxsize=15)
        self._out_queue = queue.Queue(maxsize=15)
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

        # Determine my address
        self._id = Id()
        if self._id is None:
            raise NetworkError


    def __enter__(self):
        if self._stream_uri_r:
            self._stream_r = Stream(until_eos=True,
                                    auth=self._auth).open(self._stream_uri_r, 'r')

        if self._stream_uri_w:
            self._stream_w = Stream(until_eos=True, auth=self._auth).open(self._stream_uri_w, 'w')

        time.sleep(DISCO_STARTUP_DELAY)

        executor = ThreadPoolExecutor(max_workers=2)
        recv_thrd = executor.submit(self._recv, self, log)
        send_thrd = executor.submit(self._send, self, log)

        while not self._event.is_set():
            self.discovery()

        done, not_done = wait([recv_thrd, send_thrd], return_when=concurrent.futures.ALL_COMPLETED)
        executor.shutdown()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._stream_r.close()
        self._stream_w.close()
        self._event.set()

    @staticmethod
    def _send(self, log):
        """ Encapsulate the logic/method of actually writing
        """
        #        log.debug("in _send()")
        while not self._event.is_set():
            #            log.debug("_send(): _event not set")

            for msg in [self._out_queue.get()]:
                log.debug(f"_send(): calling stream.write({msg})")
                self._stream_w.write(msg)

    @staticmethod
    def _recv(self, log):
        """ Encapsulate the logic/method of actually reading
        """
        #        log.debug("in _recv()")
        while not self._event.is_set():
            for message in self._stream_r:
                self._in_queue.put(message)

    def discovery(self):
        """ Launch the discovery protocol. Find peers.
        """
        if not self._in_disco:
            self._in_disco = True
            discorply = {"action": "DISCO", "source": self._id.getmyip()}
            self.produce(json.dumps(discorply))

        time.sleep(random.randint(2, 4))
        self.poll()

    def poll(self):
        """ Main logic for the protocol, wait for messages, register peers, end when we have enough
        """
        while not self._endit:
            time.sleep(2 + random.randint(0, 2))

            for message in self.consume():
                msg = json.loads(message.content)
                log.debug(f"in poll(): msg is {msg}}")

                if self._id.getmyip() == msg['source']:
                    log.debug("skipping message from myself")
                    continue

                if 'DISCO' in msg['action']:
                    self._in_disco = True
                    log.debug("I see DISCO. Calling reply()")
                    self.reply()
                elif 'REPLY' in msg['action']:
                    self._peerlist.add_peer(msg['reply'])
                    if len(self._peerlist) >= MIN_PEERS:
                        self.end()
                elif 'END' in msg['action']:
                    self.stop()
                else:
                    raise UnknownActionError

            if self._endit:
                break

    def reply(self):
        """ Reply to a discovery request
        """
        log.debug("reply(): sending reply")
        rply = {"action": "REPLY",
                "reply": self._id.getmyip(),
                "source": self._id.getmyip()}
        self.produce(json.dumps(rply))

    def end(self):
        """ Send the 'end' discovery protocol action
        """
        endrply = {"action": "END", "source": self._id.getmyip()}
        self.produce(json.dumps(endrply))

    def produce(self, msg: str):
        """ Put msg in the work queue
        """
        if not self._event.is_set() and not self._out_queue.full():
            log.debug(f"produce(): queueing [{msg}] for kafka")
            self._out_queue.put(str(msg))

    def consume(self):
        while not self._in_queue.empty():
            message = self._in_queue.get()
            log.debug(f"consume(): incoming message {message} from kafka")
            yield message

    def stop(self):
        self._endit = True
        self._event.set()

    def get_peerlist(self):
        """ Return the list of peers
        """
        return self._peerlist


def watchdog_timeout():
    """ Watchdog timer implementation for the discovery protocol
    """
    log.error("Watchdog time-out!")
    raise(DiscoTimeoutError)


if __name__ == "__main__":
    watchdog = threading.Timer(WATCHDOG_TIMEOUT, watchdog_timeout)
    watchdog.daemon = True
    watchdog.start()

    with Disco(broker=BROKER, read_topic=READ_TOPIC, write_topic=WRITE_TOPIC) as disco:
        log.debug(f"Peers: {disco.get_peerlist()}")

    watchdog.cancel()
