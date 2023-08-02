#!/bin/bash

set \
    -o errexit \
    -o errtrace \
    -o nounset \
    -o pipefail


cleanup() {
  echo "SIGINT caught, terminating child processes..."
  # Our cleanup code goes here
}

trap 'trap " " SIGTERM; kill 0; wait; cleanup' SIGINT SIGTERM



INTF="en7"
MYIP=`ifconfig ${INTF} | grep inet | grep netmask | awk -F" " '{ print $2 }'`

HOSTA="${MYIP}:8100"
HOSTB="${MYIP}:8101"
HOSTC="${MYIP}:8102"
HOSTD="${MYIP}:8103"


# This should be the default .env case. Let's test that.
#echo "Starting ./main.py with default .env"
echo "Starting HOSTURI=${HOSTA} PEERA_URI=${HOSTB} PEERB_URI=${HOSTC} PEERC_URI=${HOSTD} ./main.py"
( HOSTURI="${HOSTA}" PEERA_URI="${HOSTB}" PEERB_URI="${HOSTC}" PEERC_URI="${HOSTD}"  ./main.py ) &  hosta_pid=$!

echo "Starting HOSTURI=${HOSTB} PEERA_URI=${HOSTA} PEERB_URI=${HOSTC} PEERC_URI=${HOSTD} ./main.py"
( HOSTURI="${HOSTB}" PEERA_URI="${HOSTA}" PEERB_URI="${HOSTC}" PEERC_URI="${HOSTD}" ./main.py ) & hostb_pid=$!

echo "Starting HOSTURI=${HOSTC} PEERA_URI=${HOSTA} PEERB_URI=${HOSTB} PEERC_URI=${HOSTD} ./main.py"
( HOSTURI="${HOSTC}" PEERA_URI="${HOSTA}" PEERB_URI="${HOSTB}" PEERC_URI="${HOSTD}" ./main.py ) & hostc_pid=$!

echo "Starting HOSTURI=${HOSTD} PEERA_URI=${HOSTA} PEERB_URI=${HOSTB} PEERC_URI=${HOSTC} ./main.py"
( HOSTURI="${HOSTD}" PEERA_URI="${HOSTA}" PEERB_URI="${HOSTB}" PEERC_URI="${HOSTC}" ./main.py ) & hostd_pid=$!

echo "pids: ${hosta_pid} ${hostb_pid} ${hostc_pid} ${hostd_pid}"
echo "Waiting for procs to finish."
wait

echo "Done."
