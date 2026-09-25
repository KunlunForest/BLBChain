#!/bin/bash

# Create logs directory if it doesn't exist
mkdir -p logs

echo "Starting all blockchain nodes..."

# Start all shard nodes
for s in 0 1 2 3; do
    for n in 0 1 2 3; do
        echo "Starting Shard $s Node $n..."
        ./blockEmulator -n $n -N 4 -s $s -S 4 > "logs/shard${s}_node${n}.log" 2>&1 &
    done
done

# Wait a bit for nodes to start
sleep 2

# Start supervisor
echo "Starting Supervisor..."
./blockEmulator -c -N 4 -S 4 > "logs/supervisor.log" 2>&1 &

echo "All nodes started!"
echo "Check logs in the 'logs' directory"
