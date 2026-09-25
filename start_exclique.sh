#!/bin/bash

# Create logs directory if it doesn't exist
mkdir -p logs

echo "Starting ExClique blockchain nodes..."

# Start all shard nodes with ExClique
for s in 0 1 2 3; do
    for n in 0 1 2 3; do
        echo "Starting Shard $s Node $n (ExClique)..."
        ./blockEmulator -n $n -N 4 -s $s -S 4 -e > "logs/exclique_shard${s}_node${n}.log" 2>&1 &
    done
done

# Wait a bit for nodes to start
sleep 2

# Start supervisor
echo "Starting Supervisor..."
./blockEmulator -c -N 4 -S 4 > "logs/exclique_supervisor.log" 2>&1 &

echo "All ExClique nodes started!"
echo "Check logs in the 'logs' directory"
echo ""
echo "To stop all nodes, run: pkill -f blockEmulator"
