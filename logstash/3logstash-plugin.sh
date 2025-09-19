echo "########################## Printing logstash plugin list #####################################"
/usr/share/logstash/bin/logstash-plugin list

echo "########################## Printing logstash plugin list with version #####################################"
/usr/share/logstash/bin/logstash-plugin list --verbose

echo "########################## Printing logstash filter plugin list  #####################################"
/usr/share/logstash/bin/logstash-plugin list --group filter

echo "########################## Printing logstash plugins containing word metrics  #####################################"
/usr/share/logstash/bin/logstash-plugin list 'metrics'
