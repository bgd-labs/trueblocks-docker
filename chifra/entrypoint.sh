#!/bin/sh
set +e

while true; do
    chifra scrape --chain mainnet
    echo "chifra scrape exited with $?, restarting in 5s..."
    sleep 5
done &

while true; do
    chifra daemon --verbose --url 0.0.0.0:8080
    echo "chifra daemon exited with $?, restarting in 2s..."
    sleep 2
done
