#!/bin/sh
set +e

chifra init --all

while true; do
    chifra daemon --verbose --url 0.0.0.0:8080
    echo "chifra daemon exited with $?, restarting in 2s..."
    sleep 2
done
