#!/usr/bin/env python3
"""
Test implementation of a distributed lock for SNEWS Coincidence server
utilizing multiprocessing for process separation/isolation.


"""
import os
import sys
import json

from typing import List
from time import sleep
import multiprocessing as mp
from multiprocessing import Value
from rich.console import Console
from distributed.lock import DistributedLock, statedesc


def runlock(mynode: str, peerlist: List, leader_state: Value):
    """
    Create a DistributedLock instance and run it.

    :param mynode: str
    :param peerlist: List
    :param leader_state: Value
    :return: None
    """
    distributedlock = DistributedLock(mynode, peerlist, leader_state)
    distributedlock.run()


if __name__ == "__main__":
    mp.set_start_method("spawn")
    me = False
    peers = []

    console = Console()

    # Takes a json obj command line argument specifying network (of hosts) config.
    #
    if len(sys.argv) > 1:
        args = json.loads(sys.argv)

        if ("me" or "hosturi") in args.lower():
            me = args["me"]
        if "peerA" in args.lower():
            peers.append(args["peerA"])
        if "peerB" in args.lower():
            peers.append(args["peerB"])
        if "autounlocktime" in args.lower():
            autoUnlockTime = args["autoUnlockTime"]
    else:

        if "HOSTURI" in os.environ:
            me = os.environ["HOSTURI"]
        if "PEERA_URI" in os.environ:
            peers.append(os.environ["PEERA_URI"])
        if "PEERB_URI" in os.environ:
            peers.append(os.environ["PEERB_URI"])
        if "PEERC_URI" in os.environ:
            peers.append(os.environ["PEERC_URI"])

    assert me is not False
    assert len(peers) != 0

    console.log(f"I am {me}")
    console.log(f"peers are {peers}")

    LASTSTATE = None
    leader = mp.Value("i", 0, lock=True)

    p = mp.Process(target=runlock, args=(me, peers, leader))
    p.start()

    try:
        while True:
            status = leader.value
            if LASTSTATE != status:
                console.log(f"me: {me}\tstate: {statedesc[status]}")
                LASTSTATE = status

            sleep(2)

    except Exception as e:
        print(f"Encountered exception! {e}")

    finally:
        p.join()
