# Elastic Stack (Elasticsearch, Logstash, Kibana, Filebeat) Installation Guide

This guide explains how to install and configure Elastic Stack 8.x on Ubuntu.

---

## 1. Update and Upgrade System Packages
```
sudo apt update \&\& sudo apt upgrade -y
```
## 2. Install Java (Required for Elasticsearch and Logstash)
```
sudo apt install openjdk-17-jdk -y
```

### Check java version
```
java -version
```
### Set up ELK artifacts

```
sudo apt install apt-transport-https ca-certificates curl gnupg -y
curl -fsSL [https://artifacts.elastic.co/GPG-KEY-elasticsearch](https://artifacts.elastic.co/GPG-KEY-elasticsearch) | sudo gpg --dearmor -o /usr/share/keyrings/elastic-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/elastic-archive-keyring.gpg] [https://artifacts.elastic.co/packages/8.x/apt](https://artifacts.elastic.co/packages/8.x/apt) stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list
```

### Install Vim editor

```
sudo apt install vim -y
```

---

### Install Elasticsearch

```
sudo apt update
sudo apt install elasticsearch -y
```

### Edit elasticsearch configuration to disable security

```
sudo vi /etc/elasticsearch/elasticsearch.yml
```

### Update these settings to false, to disable security
```
xpack.security.enabled: false
xpack.security.transport.ssl.enabled: false
xpack.security.http.ssl.enabled: false
network.host: 0.0.0.0
```

### Set up sstemctl for elasticsearch

```
sudo systemctl daemon-reload
sudo systemctl enable elasticsearch
sudo systemctl start elasticsearch
```

### Check elastic search is running now
```
curl http://localhost:9200
```

### You should be able to see some output like this

```
{
  "name" : "Edhitas-MacBook-Air.local",
  "cluster_name" : "elasticsearch_roopal",
  "cluster_uuid" : "XskTKQYUQKKdMZSDVkY7fg",
  "version" : {
    "number" : "7.17.4",
    "build_flavor" : "default",
    "build_type" : "tar",
    "build_hash" : "79878662c54c886ae89206c685d9f1051a9d6411",
    "build_date" : "2022-05-18T18:04:20.964345128Z",
    "build_snapshot" : false,
    "lucene_version" : "8.11.1",
    "minimum_wire_compatibility_version" : "6.8.0",
    "minimum_index_compatibility_version" : "6.0.0-beta1"
  },
  "tagline" : "You Know, for Search"
}
```


---


## Install logstash

```
sudo apt install logstash -y
```

### Setup permissions

```
chmod -R 777 /usr/share/logstash/data
```

## Install filebeat

```
sudo apt install filebeat -y
```

### Setup permissions
```
chmod -R 777 /usr/share/filebeat/data
```

# Install Kibana
```
sudo apt install kibana -y
```

### Remove security from kibana

```
sudo vi /etc/kibana/kibana.yml

server.host: "0.0.0.0"
xpack.security.enabled: false
```
```
sudo systemctl enable kibana
sudo systemctl start kibana
```

# Install git

```
sudo apt install git
```

### Clone the repo
```
git clone [https://github.com/mirajgodha/elastic-stack.git](https://github.com/mirajgodha/elastic-stack.git)
```

## ✅ Installation Complete

Elasticsearch should be accessible at [http://localhost:9200](http://localhost:9200)  
Kibana should be accessible at [http://localhost:5601](http://localhost:5601)

