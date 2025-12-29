# Elastic Stack Learning Repository

A comprehensive learning resource for mastering **Elasticsearch, Logstash, and Kibana (ELK Stack)** with hands-on tutorials, configuration examples, and best practices for search, analytics, and observability implementations.

---

## Table of Contents

- [About This Repository](#about-this-repository)
- [ELK Stack Components](#elk-stack-components)
- [Getting Started](#getting-started)
- [Core Concepts](#core-concepts)
- [Use Cases](#use-cases)
- [Installation & Setup](#installation--setup)
- [Learning Paths](#learning-paths)
- [Advanced Topics](#advanced-topics)
- [Course Recommendations](#course-recommendations)
- [Contributing](#contributing)

---

## About This Repository

This repository serves as a **complete learning hub** for the Elastic Stack (ELK), covering:

✅ **Elasticsearch fundamentals** - Indexing, querying, and optimization  
✅ **Search & Analytics** - Building search relevance and behavioral analytics  
✅ **Observability** - Monitoring logs, metrics, traces, and infrastructure  
✅ **Production deployments** - Architecture, scaling, and high availability  
✅ **Real-world examples** - Configuration files, integration guides, and use cases  

Whether you're a **newcomer to log management** or preparing for **Elastic certification exams**, this resource provides structured learning with practical, executable examples.

---

## ELK Stack Components

### 1. **Elasticsearch**
The powerful search and analytics engine at the heart of the stack.

**Key capabilities:**
- Full-text search with relevance ranking
- Near real-time data indexing and retrieval
- Distributed architecture for horizontal scaling
- Advanced aggregations for analytics
- RESTful JSON API
- Built-in security (authentication, encryption, RBAC)

**Use in this repo:** Index management, query DSL examples, mapping strategies

### 2. **Logstash**
The data processing and enrichment pipeline.

**Key capabilities:**
- Multi-source data collection (logs, metrics, events)
- Powerful filtering and transformation
- Output flexibility (Elasticsearch, databases, message queues)
- Over 200 built-in plugins
- Conditional processing logic

**Use in this repo:** Configuration examples for common log sources, filter patterns, parsing strategies

### 3. **Kibana**
The visualization and exploration interface.

**Key capabilities:**
- Interactive dashboards and visualizations
- Discover UI for ad-hoc data exploration
- Advanced alerting and anomaly detection
- ES|QL query language for powerful searches
- Canvas for custom reporting

**Use in this repo:** Dashboard examples, visualization patterns, monitoring setup

---

## Getting Started

### Prerequisites

Before diving into this learning resource, ensure you have:

- **Linux/macOS/Windows** with Docker or native Java 11+ installed
- **Basic command-line proficiency**
- **Understanding of JSON** format
- **2GB+ RAM** for local development setup

### Quick Start (5 Minutes)

#### Option 1: Docker Compose (Recommended for Learning)

```bash
# Clone this repository
git clone https://github.com/mirajgodha/elastic-stack.git
cd elastic-stack

# Deploy ELK Stack with Docker
docker-compose up -d

# Verify installation
curl http://localhost:9200
curl http://localhost:5601  # Kibana at http://localhost:5601
```

#### Option 2: Native Installation

For Ubuntu/Debian-based systems:

```bash
# Install Elasticsearch
curl -fsSL https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
echo "deb https://artifacts.elastic.co/packages/8.x/apt stable main" | \
  sudo tee -a /etc/apt/sources.list.d/elastic-8.x.list

sudo apt-get update
sudo apt-get install elasticsearch kibana logstash

# Start services
sudo systemctl start elasticsearch
sudo systemctl start kibana
sudo systemctl start logstash
```

**Verification:**
- Elasticsearch: `curl http://localhost:9200` → Returns cluster info
- Kibana: Visit `http://localhost:5601` in your browser
- Logstash: Check `/var/log/logstash/logstash-plain.log`

---

## Core Concepts

### Indexing & Documents

Elasticsearch stores data as **JSON documents** organized in **indices** (analogous to database tables):

```json
{
  "user": "john_doe",
  "timestamp": "2025-01-10T14:30:00Z",
  "action": "login",
  "ip_address": "192.168.1.1",
  "response_time_ms": 145
}
```

**Key concepts:**
- **Index**: Logical container for documents (e.g., `logs-2025-01-10`)
- **Document**: Individual record with unique `_id`
- **Shard**: Physical partition of an index for scaling
- **Replica**: Copy of a shard for fault tolerance
- **Mapping**: Schema defining field types and analyzers

### Query DSL (Domain Specific Language)

Elasticsearch uses a powerful, flexible query syntax:

```json
{
  "query": {
    "bool": {
      "must": [
        { "match": { "action": "login" } }
      ],
      "filter": [
        { "range": { "timestamp": { "gte": "2025-01-01" } } },
        { "term": { "status": "success" } }
      ]
    }
  }
}
```

**Common query types:**
- `match` - Full-text search with scoring
- `term` - Exact value matching
- `range` - Numeric or date range filtering
- `bool` - Combine multiple queries with AND/OR logic
- `aggregation` - Group and summarize data

### Analysis & Tokenization

Text fields go through **analysis** to break into searchable tokens:

```
Input: "The quick brown fox"
↓ (Tokenizer)
["The", "quick", "brown", "fox"]
↓ (Token filters)
["the", "quick", "brown", "fox"]  # lowercased
```

Proper analyzer configuration improves search relevance.

---

## Use Cases

### 1. **Log Management & Monitoring**

Centralize logs from all applications and infrastructure for real-time visibility:

**What you'll learn:**
- Collecting logs via Filebeat/Logstash from servers
- Parsing structured and unstructured logs
- Creating observability dashboards
- Setting up alerts for critical errors
- Troubleshooting performance issues

**Example workflow:**
```
Nginx logs → Filebeat → Logstash (parse) → Elasticsearch → Kibana dashboard
```

### 2. **Search & Analytics**

Build powerful search engines with intelligent relevance and behavioral insights:

**What you'll learn:**
- Creating search indices with proper mappings
- Implementing full-text search with relevance tuning
- Building behavioral analytics dashboards
- Analyzing search query patterns
- Improving search results with synonyms and boosting

**Example dashboard metrics:**
- Query volume and trends
- Click-through rates (CTR)
- Top search queries
- Search abandonment rates
- Result relevance metrics

### 3. **Infrastructure & Application Observability**

Monitor the health and performance of your entire stack:

**What you'll learn:**
- Collecting metrics from hosts, containers, and Kubernetes
- Application Performance Monitoring (APM)
- Distributed tracing with OpenTelemetry
- Infrastructure health dashboards
- Alerting on anomalies and performance degradation

**Key metrics tracked:**
- CPU, memory, disk utilization
- Network I/O and latency
- Application response times
- Error rates and exceptions
- Database query performance

### 4. **Security & Threat Detection**

Detect and respond to security threats with log analysis:

**What you'll learn:**
- Parsing security logs (firewall, IDS, authentication)
- Detecting suspicious patterns
- Building threat intelligence dashboards
- Creating security alerts
- Compliance reporting

---

## Installation & Setup

### Production-Grade Architecture

For real-world deployments, consider this architecture:

```
┌─────────────────────────────────────────────────┐
│                Data Sources                      │
│  (Apps, Servers, Containers, APIs)              │
└──────────────────┬──────────────────────────────┘
                   │
        ┌──────────▼──────────┐
        │   Data Collection   │
        │ (Filebeat/Metricbeat│
        │  /Logstash)         │
        └──────────┬──────────┘
                   │
    ┌──────────────▼──────────────┐
    │   Elasticsearch Cluster     │
    │  ┌────────────────────────┐ │
    │  │ Node 1 (Master+Data)   │ │
    │  │ Node 2 (Master+Data)   │ │
    │  │ Node 3 (Master+Data)   │ │
    │  └────────────────────────┘ │
    │  (Sharding & Replication)   │
    └──────────────┬──────────────┘
                   │
    ┌──────────────▼──────────────┐
    │  Kibana (Visualization)     │
    │  Alerting & Reporting       │
    └─────────────────────────────┘
```

### Configuration Best Practices

#### Elasticsearch Configuration (`elasticsearch.yml`)

```yaml
# Cluster settings
cluster.name: production-cluster
cluster.initial_master_nodes:
  - node-1
  - node-2
  - node-3

# Node settings
node.name: node-1
node.roles: [master, data, ingest]

# Memory allocation (JVM)
# Edit jvm.options: -Xms4g -Xmx4g (50% of total RAM, max 32GB)

# Network
network.host: 0.0.0.0
http.port: 9200
transport.port: 9300

# Index settings
index.number_of_shards: 5
index.number_of_replicas: 1

# Security (X-Pack)
xpack.security.enabled: true
xpack.security.enrollment.enabled: true

# Performance
indices.memory.index_buffer_size: 40%
thread_pool.search.queue_size: 1000
```

#### Logstash Pipeline Configuration

```conf
input {
  beats {
    port => 5000
    codec => json
  }
}

filter {
  # Parse timestamps
  date {
    match => [ "timestamp", "ISO8601" ]
    target => "@timestamp"
  }

  # Extract fields
  grok {
    match => { "message" => "%{COMBINEDAPACHELOG}" }
  }

  # Filter out noise
  if [status] =~ /^(200|304)$/ and [request_path] =~ /^\/(health|ping)$/ {
    drop { }
  }

  # Add enrichment
  geoip {
    source => "client_ip"
  }

  # Mutate
  mutate {
    convert => { "response_time" => "integer" }
    remove_field => [ "message" ]
  }
}

output {
  elasticsearch {
    hosts => ["localhost:9200"]
    index => "logs-%{+YYYY.MM.dd}"
    user => "logstash_user"
    password => "${LOGSTASH_PASSWORD}"
  }
}
```

### Index Template Strategy

Create index templates for automated index management:

```json
{
  "index_patterns": ["logs-*"],
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1,
    "index.lifecycle.name": "log-retention-policy"
  },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "level": { "type": "keyword" },
      "message": { "type": "text" },
      "service": { "type": "keyword" },
      "duration_ms": { "type": "long" }
    }
  }
}
```

---

## Learning Paths

### Path 1: Log Management Specialist (8-12 weeks)

Perfect for DevOps engineers, system administrators, and platform teams.

**Week 1-2: Fundamentals**
- ELK Stack architecture and components
- Elasticsearch basics (indexing, querying, mapping)
- Installing and configuring Elasticsearch locally

**Week 3-4: Data Collection**
- Filebeat for log shipping
- Logstash pipeline creation and filters
- Parsing common log formats (JSON, syslog, Apache)

**Week 5-6: Observability**
- Creating operational dashboards in Kibana
- Setting up alerts for critical metrics
- Using ES|QL for advanced searching
- Monitoring log sources and pipelines

**Week 7-8: Advanced Operations**
- Cluster management and scaling
- Index lifecycle management (ILM)
- Performance tuning and optimization
- Backup and recovery strategies

**Week 9-12: Production Deployment**
- Security hardening and authentication
- High-availability architecture
- Disaster recovery planning
- Real-world case studies and troubleshooting

**Capstone Project:** Deploy a fully monitored multi-tier application stack

### Path 2: Search & Analytics Engineer (10-14 weeks)

Ideal for search engineers, data engineers, and product teams.

**Week 1-3: Elasticsearch Fundamentals**
- Advanced query DSL
- Relevance tuning and scoring
- Custom analyzers and tokenization
- Aggregations and metrics

**Week 4-5: Search Application Development**
- Building search indices
- Implementing autocomplete and suggestions
- Typo tolerance and fuzzy matching
- Search result ranking and personalization

**Week 6-7: Analytics & Insights**
- Behavioral analytics implementation
- Query and click-through analytics
- Building custom dashboards
- A/B testing search changes

**Week 8-10: Performance Optimization**
- Query optimization techniques
- Index optimization strategies
- Caching and performance tuning
- Capacity planning and scaling

**Week 11-14: Advanced Features**
- Machine learning for anomaly detection
- Semantic search with vector embeddings
- Search quality metrics and monitoring
- Production search systems architecture

**Capstone Project:** Build a search application with analytics and relevance optimization

### Path 3: Observability & Reliability Engineer (12-16 weeks)

Designed for SREs, infrastructure engineers, and operations teams.

**Week 1-2: Observability Foundations**
- Three pillars of observability (logs, metrics, traces)
- OpenTelemetry integration
- Telemetry collection strategies

**Week 3-5: Metrics & Infrastructure Monitoring**
- Metricbeat for infrastructure metrics
- Prometheus to Elasticsearch integration
- Host and container monitoring
- Resource utilization dashboards

**Week 6-8: Logs & Event Data**
- Advanced log parsing and enrichment
- Structured logging best practices
- Anomaly detection with Elastic ML
- Root cause analysis workflows

**Week 9-11: Distributed Tracing & APM**
- Application Performance Monitoring setup
- OpenTelemetry instrumentation
- Trace sampling strategies
- Service dependency mapping

**Week 12-16: Incident Response & SLOs**
- Alert design and tuning
- Runbook integration
- Service Level Objectives (SLOs)
- On-call automation and escalation
- Post-incident analysis

**Capstone Project:** Design a complete observability platform for microservices

---

## Advanced Topics

### 1. Elasticsearch Query Optimization

**Problem:** Slow queries impacting dashboard performance

**Solutions in this repo:**
- Query profiling techniques
- Index design for query performance
- Filter cache optimization
- Query DSL best practices
- Pagination strategies

```bash
# Profile a query
curl -X GET "localhost:9200/logs-*/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "profile": true,
    "query": { "match": { "message": "error" } }
  }'
```

### 2. Index Lifecycle Management (ILM)

Automate index rollover, optimization, and deletion:

```json
{
  "policy": "log-retention",
  "phases": {
    "hot": {
      "min_age": "0d",
      "actions": {
        "rollover": { "max_primary_shard_size": "50GB" }
      }
    },
    "warm": {
      "min_age": "7d",
      "actions": {
        "set_priority": { "priority": 50 }
      }
    },
    "cold": {
      "min_age": "30d",
      "actions": {
        "searchable_snapshot": {}
      }
    },
    "delete": {
      "min_age": "90d",
      "actions": {
        "delete": {}
      }
    }
  }
}
```

### 3. Cluster Health & Monitoring

**Key metrics to track:**
- Cluster health status (green/yellow/red)
- Unassigned shards
- Active shards per node
- JVM heap usage
- Indexing rate and latency
- Query latency percentiles

```bash
# Monitor cluster health
curl -X GET "localhost:9200/_cluster/health?pretty"

# Get detailed stats
curl -X GET "localhost:9200/_nodes/stats?pretty"
```

### 4. Security Implementation

**Components:**
- Role-based access control (RBAC)
- User authentication
- Index-level security
- Field-level security
- Audit logging

```yaml
# Create a read-only user for monitoring
POST /_security/user/monitoring_user
{
  "password": "secure_password",
  "roles": ["monitoring_user"],
  "full_name": "Monitoring User"
}
```

### 5. Machine Learning Features

Use Elasticsearch ML for:
- Anomaly detection in metrics
- Forecasting future trends
- Unusual event detection
- Categorization of log messages
- Job scheduling and automation

---

## Course Recommendations

### 🎓 Comprehensive Learning with Expert Instruction

To accelerate your mastery of the Elastic Stack and prepare for certification, we recommend the following courses from **QuantumRoot**:

#### **1. Elasticsearch Search & Analytics Specialist Course**

**Course URL:** [https://quantumroot.in/courses/elasticsearch-search-analytics](https://quantumroot.in/courses/elasticsearch-search-analytics)

**Perfect for:** Search engineers, product teams, data engineers wanting to build intelligent search systems

**What you'll master:**
- ✅ Advanced Elasticsearch query optimization
- ✅ Building search relevance from scratch
- ✅ Implementing behavioral analytics dashboards
- ✅ Search quality metrics and monitoring
- ✅ A/B testing and continuous improvement
- ✅ Semantic search with vector embeddings
- ✅ Production search architecture patterns
- ✅ Real-world search system case studies

**Certification:** Prepare for **Elastic Certified Analyst** exam

**Duration:** 12-16 weeks (flexible, self-paced)

**Key Benefits:**
- Hands-on labs with real datasets
- Industry-standard best practices
- Access to instructors for guidance
- Certificate of completion recognized in the industry

---

#### **2. Elastic ELK Stack Observability Engineer Certification Course**

**Course URL:** [https://quantumroot.in/courses/elastic-elk-observability-engineer-certification-course](https://quantumroot.in/courses/elastic-elk-observability-engineer-certification-course)

**Perfect for:** DevOps engineers, SREs, platform engineers, system administrators

**What you'll master:**
- ✅ Complete Elastic Stack architecture and deployment
- ✅ Infrastructure monitoring with Metricbeat
- ✅ Application Performance Monitoring (APM)
- ✅ OpenTelemetry integration and instrumentation
- ✅ Distributed tracing and root cause analysis
- ✅ Advanced alerting and anomaly detection
- ✅ Service Level Objectives (SLOs) and reliability
- ✅ Kubernetes and container monitoring
- ✅ Incident response automation
- ✅ Production deployment strategies

**Certification:** **Elastic Certified Observability Engineer** - Industry-recognized credential

**Duration:** 14-18 weeks (flexible, self-paced)

**Key Benefits:**
- Complete observability platform design
- Real-world incident scenarios
- Multi-cluster management
- Advanced troubleshooting techniques
- Exam practice tests included
- Career advancement with official certification

---

### Why Choose These Courses?

| Aspect | Benefit |
|--------|---------|
| **Expert Instruction** | Learn from certified Elastic professionals with years of production experience |
| **Hands-On Labs** | 50+ practical labs with real datasets and production scenarios |
| **Certification Prep** | Directly aligned with official Elastic certification exams |
| **Industry Recognition** | Credentials valued by top tech companies globally |
| **Career Growth** | Certified professionals earn 20-30% higher salaries on average |
| **Lifetime Access** | Course materials, updates, and community support forever |
| **Job Placement** | Access to QuantumRoot's network of hiring partners |

---

### Learning Paths Aligned with Courses

**For Search & Analytics Focus:**
1. Start with this repository's search optimization section
2. Enroll in the **Search & Analytics course**
3. Build a search project using real e-commerce or content data
4. Pass the **Elastic Certified Analyst exam**

**For Observability & Operations Focus:**
1. Begin with this repository's log management and monitoring sections
2. Enroll in the **Observability Engineer course**
3. Deploy a production observability stack
4. Earn the **Elastic Certified Observability Engineer credential**

**For Full Mastery (Recommended):**
1. Complete both courses sequentially
2. Gain expertise across all ELK Stack components
3. Become a versatile Elastic engineer
4. Unlock advanced career opportunities

---

## Repository Structure

```
elastic-stack/
├── README.md                          # This file
├── docker-compose.yml                 # Quick local setup
├── elasticsearch/
│   ├── config/
│   │   └── elasticsearch.yml          # Configuration reference
│   ├── mappings/
│   │   ├── logs-mapping.json          # Log index mapping
│   │   ├── metrics-mapping.json       # Metrics mapping
│   │   └── search-mapping.json        # Search index mapping
│   ├── index-templates/
│   │   ├── logs-template.json         # Auto-create log indices
│   │   └── metrics-template.json      # Auto-create metric indices
│   └── queries/
│       ├── basic-queries.md           # Query DSL examples
│       ├── aggregations.md            # Analytics aggregations
│       └── optimization.md            # Performance tips
├── logstash/
│   ├── pipelines/
│   │   ├── apache-logs.conf           # Apache/Nginx parsing
│   │   ├── json-logs.conf             # JSON log processing
│   │   ├── syslog.conf                # System log parsing
│   │   └── multi-line.conf            # Complex log handling
│   ├── filters/
│   │   ├── grok-patterns.md           # Custom patterns
│   │   └── enrich-examples.md         # Data enrichment
│   └── logstash.yml                   # Configuration reference
├── kibana/
│   ├── dashboards/
│   │   ├── observability.json         # Ops monitoring dashboard
│   │   ├── search-analytics.json      # Search metrics
│   │   └── security.json              # Security events
│   ├── visualizations/
│   │   ├── charts-examples.md         # Visualization types
│   │   └── canvas-examples.md         # Canvas tutorials
│   └── saved-searches/
│       ├── log-analysis.json          # Common search patterns
│       └── troubleshooting.json       # Debugging queries
├── filebeat/
│   ├── filebeat.yml                   # Configuration
│   └── modules/
│       ├── nginx-config.yml           # Nginx log collection
│       ├── system-config.yml          # System metrics
│       └── docker-config.yml          # Container logs
├── metricbeat/
│   ├── metricbeat.yml                 # Configuration
│   └── modules/
│       ├── system.yml                 # Host metrics
│       ├── docker.yml                 # Container metrics
│       └── elasticsearch.yml          # Cluster monitoring
├── examples/
│   ├── log-aggregation/               # Centralized logging setup
│   ├── search-engine/                 # Search implementation
│   ├── apm-setup/                     # Application monitoring
│   └── alerting/                      # Alert configuration
├── scripts/
│   ├── setup.sh                       # Automated installation
│   ├── backup.sh                      # Snapshot automation
│   └── test-queries.sh                # Query testing
└── docs/
    ├── architecture.md                # System design
    ├── troubleshooting.md             # Common issues
    ├── performance-tuning.md          # Optimization guide
    ├── security.md                    # Security hardening
    └── elk-certification-guide.md     # Exam preparation
```

---

## Common Use Cases & Examples

### Use Case 1: Centralized Log Management

**Scenario:** Monitor logs from 50+ microservices in Kubernetes

**Architecture:**
```
Kubernetes Pods
    ↓
Filebeat (DaemonSet)
    ↓
Logstash (Stateless)
    ↓
Elasticsearch (Cluster)
    ↓
Kibana (Discovery & Alerting)
```

**See examples in:** `examples/log-aggregation/`

### Use Case 2: Building a Search Engine

**Scenario:** E-commerce search with relevance ranking and autocomplete

**Key features:**
- Fuzzy matching for typo tolerance
- Custom scoring based on popularity
- Real-time suggestions
- Search analytics tracking

**See examples in:** `examples/search-engine/`

### Use Case 3: Application Performance Monitoring

**Scenario:** Monitor microservices performance, latency, and errors

**Metrics tracked:**
- Request latency (p50, p95, p99)
- Error rates by service
- Database query performance
- External API response times

**See examples in:** `examples/apm-setup/`

### Use Case 4: Alerting & Incident Response

**Scenario:** Detect anomalies and notify teams automatically

**Alert types:**
- Threshold-based (e.g., error rate > 5%)
- Anomaly detection (unusual patterns)
- Composite alerts (multiple conditions)
- ML-powered predictions

**See examples in:** `examples/alerting/`

---

## Troubleshooting Guide

### Common Issues

#### 1. Elasticsearch Won't Start

**Problem:** Elasticsearch crashes immediately after startup

**Solutions:**
- Check JVM memory: `elasticsearch.yml` should have `-Xms` and `-Xmx` set to ≤50% of available RAM
- Verify disk space: Need at least 1GB free (preferably 10GB+)
- Check network binding: Ensure ports 9200 and 9300 are available
- Review logs: `journalctl -u elasticsearch -f` or logs in `/var/log/elasticsearch/`

#### 2. Slow Queries in Kibana

**Problem:** Dashboards taking >5 seconds to load

**Solutions:**
- Profile the query: Use `_profile` endpoint
- Add filters to reduce data scanned
- Increase shard count for parallelization
- Use aggregations instead of all raw data
- Consider index optimization or sampling

#### 3. Logstash Not Processing Events

**Problem:** Events not appearing in Elasticsearch

**Solutions:**
- Verify Logstash pipeline: `bin/logstash -f pipeline.conf --config.test_and_exit`
- Check logs: Monitor Logstash log output
- Verify Elasticsearch connectivity: Ensure `elasticsearch` host and credentials are correct
- Test input source: Send test data manually

#### 4. High Memory Usage

**Problem:** JVM heap continuously growing or OOM errors

**Solutions:**
- Reduce `indices.memory.index_buffer_size` in Elasticsearch
- Lower `bulk_size` in Logstash
- Implement aggressive index lifecycle management
- Scale horizontally with more nodes
- Monitor with `_nodes/stats` endpoint

#### 5. Yellow Cluster Health

**Problem:** Replicas not being assigned

**Solutions:**
- Check available nodes: Unbalanced clusters may prevent replica placement
- Verify shard allocation: `_cluster/allocation/explain`
- Increase node count if necessary
- Review `index.number_of_replicas` setting

---

## Performance Tuning Checklist

### Elasticsearch Optimization

- [ ] JVM heap set to 4-16GB (not more than 32GB)
- [ ] Heap to non-heap ratio properly balanced
- [ ] GC logging enabled for monitoring
- [ ] Swap disabled to prevent GC pauses
- [ ] Number of shards optimized (3-5 per 2GB heap)
- [ ] Proper mapping with correct field types
- [ ] Index statistics updated regularly
- [ ] Hot-warm-cold architecture implemented for time-series data

### Logstash Optimization

- [ ] `batch.size` configured (50-100 typical)
- [ ] `pipeline.workers` matches CPU count
- [ ] Grok patterns optimized for your data
- [ ] Unnecessary filters removed
- [ ] Bulk API enabled for Elasticsearch output
- [ ] Dead letter queue configured
- [ ] Monitoring enabled with metrics

### Kibana Optimization

- [ ] Index pattern configured efficiently
- [ ] Saved searches cached
- [ ] Dashboard queries optimized
- [ ] Visualization sampling used for large datasets
- [ ] Canvas reports use timeseries data
- [ ] Browser caching enabled

---

## Security Hardening

### Essential Security Steps

1. **Enable X-Pack Security**
   ```yaml
   xpack.security.enabled: true
   xpack.security.transport.ssl.enabled: true
   xpack.security.http.ssl.enabled: true
   ```

2. **Configure TLS/SSL**
   - Generate certificates using `elasticsearch-certutil`
   - Configure for transport and HTTP layers
   - Verify certificate validity

3. **Create Users and Roles**
   ```bash
   # Create a user
   curl -X POST "localhost:9200/_security/user/app_user?pretty" \
     -H 'Content-Type: application/json' \
     -d '{
       "password": "l=Zs{3Edta9ZjISb",
       "roles": ["app_role"],
       "full_name": "Application User"
     }'
   ```

4. **Implement Network Security**
   - Firewall rules to restrict access
   - Private network for cluster communication
   - VPN or bastion hosts for remote access

5. **Enable Audit Logging**
   ```yaml
   xpack.security.audit.enabled: true
   xpack.security.audit.logfile.events.include:
     - access_denied
     - access_granted
     - connection_granted
   ```

---

## Contributing

We welcome contributions! Areas we need help with:

- [ ] Additional Logstash pipeline configurations
- [ ] Dashboard examples for different use cases
- [ ] Performance tuning documentation
- [ ] Multi-language client examples
- [ ] Container orchestration guides
- [ ] Troubleshooting scenarios and solutions
- [ ] Certification exam preparation materials

**To contribute:**
1. Fork the repository
2. Create a feature branch
3. Add your examples/documentation
4. Submit a pull request with clear descriptions

---

## Resources & References

### Official Documentation
- [Elasticsearch Docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Kibana Docs](https://www.elastic.co/guide/en/kibana/current/index.html)
- [Logstash Docs](https://www.elastic.co/guide/en/logstash/current/index.html)

### Learning Resources
- [Elastic Training Programs](https://www.elastic.co/training)
- [Elasticsearch Learning Path](https://www.elastic.co/learn)
- [Elastic Certification](https://www.elastic.co/certification)

### Community
- [Elastic Community Slack](https://elasticstack.slack.com)
- [Elastic Discuss Forum](https://discuss.elastic.co)
- [Stack Overflow #elasticsearch](https://stackoverflow.com/questions/tagged/elasticsearch)

---

## Certification Preparation

This repository complements official Elastic certification exams:

### Elastic Certified Analyst
- Search and query optimization
- Relevance tuning
- Analytics implementation
- Practical lab environment

### Elastic Certified Observability Engineer
- Infrastructure monitoring
- Log aggregation and analysis
- Application performance monitoring
- Production deployment and management

**Recommended Path:**
1. Study this repository's examples
2. Enroll in corresponding QuantumRoot courses
3. Complete all hands-on labs and projects
4. Take practice exams
5. Schedule official certification exam

---

## License

This learning repository is provided under the [MIT License](LICENSE).

---

## Support & Contact

For questions about:
- **This repository:** Open an issue on GitHub
- **Course enrollment:** Visit [QuantumRoot](https://quantumroot.in)
- **Certification exam prep:** Contact Elastic directly
- **Technical issues:** Check troubleshooting guide or community forums

---

## Stay Updated

- ⭐ Star this repository to stay updated
- 🔔 Watch for new examples and documentation
- 📧 Subscribe for release notifications
- 💬 Participate in discussions and issues

---

**Last Updated:** January 2025  
**Maintainer:** Miraj Godha  
**Elastic Stack Version:** 8.0+

---

## Acknowledgments

This repository is built on:
- Official Elastic documentation and best practices
- Community examples and patterns
- Real-world production experiences
- Feedback from Elastic engineers and community members

Thank you for using this learning resource. Happy learning and building with the Elastic Stack! 🚀
