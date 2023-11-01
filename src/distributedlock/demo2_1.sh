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

cd /container/apps/distributedlock/

# This should be the default .env case. Let's test that.
echo "Starting ./main.py --env=.env.2"
( ./main.py --env=.env.2 ) &  hosta_pid=$!

echo "Starting ./main.py --env=.env.3"
( ./main.py --env=.env.3 ) & hostb_pid=$!

echo "pids: ${hosta_pid} ${hostb_pid}"
echo "Waiting for procs to finish."
wait

echo "Done."
