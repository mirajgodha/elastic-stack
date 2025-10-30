#!/bin/bash
# Troubleshooting script for Logstash ingestion issues

echo "=========================================="
echo "Logstash Troubleshooting Script"
echo "=========================================="

echo ""
echo "1. Checking Elasticsearch cluster health..."
curl -X GET "localhost:9200/_cluster/health?pretty"

echo ""
echo "2. Checking all devops-tools indices..."
curl -X GET "localhost:9200/_cat/indices/devops-tools-*?v"

echo ""
echo "3. Checking document count per index..."
curl -X GET "localhost:9200/devops-tools-*/_count?pretty"

echo ""
echo "4. Checking log files exist and are readable..."
ls -lh /var/log/devops-tools/

echo ""
echo "5. Checking file line counts..."
for file in /var/log/devops-tools/*.json; do
    echo "$(wc -l < $file) lines in $file"
done

echo ""
echo "6. Checking Logstash logs for errors..."
tail -n 50 /var/log/logstash/logstash-plain.log 2>/dev/null || \
tail -n 50 /opt/homebrew/var/log/logstash/logstash-plain.log 2>/dev/null || \
echo "Could not find Logstash logs"

echo ""
echo "7. Checking if Logstash is running..."
ps aux | grep logstash | grep -v grep

echo ""
echo "8. Sample document from each index..."
for index in $(curl -s "localhost:9200/_cat/indices/devops-tools-*?h=index" | head -5); do
    echo ""
    echo "Sample from $index:"
    curl -X GET "localhost:9200/$index/_search?size=1&pretty"
done

echo ""
echo "=========================================="
echo "Troubleshooting complete"
echo "=========================================="
