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

exec chifra daemon --verbose --url 0.0.0.0:8080
