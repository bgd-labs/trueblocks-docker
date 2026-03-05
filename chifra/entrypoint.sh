#!/bin/sh
set -e

CONFIG="/root/.local/share/trueblocks/trueBlocks.toml"

# Extract all chain names from [chains.<name>] section headers
chains=$(grep '^\[chains\.' "$CONFIG" | sed 's/\[chains\.\(.*\)\]/\1/')

for chain in $chains; do
    manifest="/unchained/${chain}/manifest.json"
    if [ ! -f "$manifest" ]; then
        echo "No manifest found for chain '${chain}', running chifra init..."
        chifra init --chain "$chain" &
    fi
done

wait

set +e

for chain in $chains; do
    while true; do
        chifra scrape --chain "$chain"
        echo "chifra scrape for $chain exited with $?, restarting in 5s..."
        sleep 5
    done &
done

while true; do
    chifra daemon --verbose --url 0.0.0.0:8080
    echo "chifra daemon exited with $?, restarting in 2s..."
    sleep 2
done
