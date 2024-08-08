#!/usr/bin/env python3
"""
Test implementation of a distributed lock for SNEWS Coincidence server
utilizing multiprocessing for process separation/isolation.


"""
import os
from typing import List
from time import sleep
import multiprocessing as mp
from multiprocessing import Value

from dotenv import load_dotenv
import click
from rich.console import Console
from distributed.lock import (DistributedLock, statedesc,
                               InvalidPeerArgumentError, InvalidHostURIError)
#from distributed.disco import Disco
from distributed.disco import Disco, BROKER, READ_TOPIC, WRITE_TOPIC, DiscoTimeoutError

def runlock(mynode: str, peerlist: List, leader_state: Value):
    """
    Create a DistributedLock instance and run it.

    :param mynode: str
    :param peerlist: List
    :param leader_state: Value
    :return: None
    """
    distributedlock = DistributedLock(mynode, peerlist, leader=leader_state)
    distributedlock.run()


@click.group(invoke_without_command=True)
@click.option('--env', type=str, default='.env',
    show_default='.env', help='environment file containing the host/peer configuration')
@click.pass_context
def main(ctx=None, env=None):
    mp.set_start_method("spawn")
    myhosturi = False
    peers = []

    console = Console()

    ctx.ensure_object(dict)
    envf = env or '.env'
    load_dotenv(envf)
    ctx.obj['env'] = env


    # Discover other distributed lock servers via Kafka.
    #
    #    XXX - TODO -
    #    This is the only dependency on Kafka. Can this be abstracted out? Or should this library
    #    be more tightly integrated with SNEWS Coincidence server code?
    #
    #    Also, right now we only negotiate this info once, at startup. This needs to be
    #    more flexible. Peers may come and go. We need to be able to drop back into discovery mode
    #    as peers come and go, perhaps even asynchronously.

    with Disco(broker=BROKER, read_topic=READ_TOPIC, write_topic=WRITE_TOPIC) as disco:
        myhosturi = disco.whoami()
        print(f"discovery state: {disco.get_peerlist()}")
        peers.append(disco.get_peerlist())

    if not myhosturi:
        raise InvalidHostURIError

    if len(peers) < 3 or None in peers:
        raise InvalidPeerArgumentError

    console.log(f"I am {myhosturi}")
    console.log(f"peers are {peers}")

    laststate = None
    leader = mp.Value("i", 0, lock=True)

    p = mp.Process(target=runlock, args=(myhosturi, peers, leader))
    p.start()

    try:
        while True:
            status = leader.value
            if laststate != status:
                console.log(f"me: {myhosturi}\tstate: {statedesc[status]}")
                laststate = status

            sleep(2)

    except Exception:
        console.print_exception()

    finally:
        p.join()


if __name__ == "__main__":
    main()
