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

# This should be the default .env case. Let's test that.
echo "Starting ./main.py with default .env"
( ./main.py --env=.env ) &  hosta_pid=$!

echo "Starting ./main.py --env=.env.1"
( ./main.py --env=.env.1 ) & hostb_pid=$!

echo "pids: ${hosta_pid} ${hostb_pid}"
echo "Waiting for procs to finish."
wait

echo "Done."
