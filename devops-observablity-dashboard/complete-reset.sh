#!/bin/bash
# Complete Logstash Reset and Data Load

echo "==========================================="
echo "Complete Logstash Reset & Data Load"
echo "==========================================="

# Step 1: Delete all old indices
echo ""
echo "Step 1: Cleaning up old indices..."
curl -X DELETE "localhost:9200/devops-tools-*?pretty"

# Step 2: Remove sincedb tracking files
echo ""
echo "Step 2: Clearing Logstash file tracking..."
rm -f /tmp/logstash-*.sincedb
rm -rf ~/.sincedb_*
rm -f /opt/homebrew/var/lib/logstash/plugins/inputs/file/.sincedb_* 2>/dev/null
rm -f /var/lib/logstash/plugins/inputs/file/.sincedb_* 2>/dev/null

# Step 3: Kill any running Logstash
echo ""
echo "Step 3: Stopping any running Logstash..."
pkill -9 -f logstash
sleep 2

# Step 4: Verify log files
echo ""
echo "Step 4: Verifying log files..."
for file in /var/log/devops-tools/*.json; do
    lines=$(wc -l < "$file")
    basename=$(basename "$file")
    echo "  $basename: $lines lines"
done

# Step 5: Change ownership to current user (for macOS homebrew)
echo ""
echo "Step 5: Fixing file permissions..."
sudo chown -R $(whoami):staff /var/log/devops-tools/
chmod -R 644 /var/log/devops-tools/*.json

echo ""
echo "Step 6: Running Logstash to ingest all files..."
echo "This will take about 60 seconds. Watch for 'Pipeline started' message."
echo ""

# Run Logstash in foreground, let it process, then kill it
/opt/homebrew/Cellar/logstash/9.1.5/bin/logstash \
  -f /Users/roopal/Downloads/elk/logstash-config.conf \
  2>&1 | tee /tmp/logstash-output.log &

LOGSTASH_PID=$!

# Wait for pipeline to start
echo "Waiting for Logstash pipeline to start..."
timeout=60
counter=0
while [ $counter -lt $timeout ]; do
    if grep -q "Pipeline started" /tmp/logstash-output.log 2>/dev/null; then
        echo "✓ Pipeline started!"
        break
    fi
    sleep 1
    counter=$((counter + 1))
    echo -n "."
done
echo ""

# Give it time to process files
echo "Processing files (30 seconds)..."
sleep 30

# Stop Logstash gracefully
echo "Stopping Logstash..."
kill $LOGSTASH_PID 2>/dev/null
wait $LOGSTASH_PID 2>/dev/null
sleep 3

# Force kill if still running
pkill -9 -f logstash 2>/dev/null

echo ""
echo "Step 7: Checking results..."
echo ""

# Check document counts
curl -s "localhost:9200/_cat/indices/devops-tools-*?v&s=index"

echo ""
echo ""
echo "Total document count:"
curl -s "localhost:9200/devops-tools-*/_count?pretty" | grep -A1 "count"

echo ""
echo ""
echo "Documents per service:"
for service in jenkins sonarqube nexus-iq twistlock confluence jira openshift ansible; do
    count=$(curl -s "localhost:9200/devops-tools-${service}-*/_count" | grep -o '"count":[0-9]*' | cut -d':' -f2)
    echo "  $service: $count documents"
done

echo ""
echo "==========================================="
echo "Expected: 400 documents (50 per service)"
echo "==========================================="
