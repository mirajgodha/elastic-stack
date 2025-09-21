# =============================================================================
# INDEX MANAGEMENT - TEMPLATES, ALIASES, REINDEXING, PLUGINS, BACKUP/RESTORE
# =============================================================================
# Essential Elasticsearch index management operations

# INDEX TEMPLATES - Define settings and mappings for new indices
# ==============================================================

# Example 1: Create an index template for product indices
PUT /_index_template/product_template
{
  "index_patterns": ["products-*"],
  "priority": 100,
  "template": {
    "settings": {
      "number_of_shards": 2,
      "number_of_replicas": 1,
      "analysis": {
        "analyzer": {
          "product_analyzer": {
            "type": "standard",
            "stopwords": ["the", "a", "an"]
          }
        }
      }
    },
    "mappings": {
      "properties": {
        "name": {
          "type": "text",
          "analyzer": "product_analyzer"
        },
        "price": {
          "type": "double"
        },
        "category": {
          "type": "keyword"
        },
        "created_at": {
          "type": "date"
        }
      }
    }
  }
}

# Example 2: Create index template with component templates
PUT /_component_template/product_settings
{
  "template": {
    "settings": {
      "index.number_of_shards": 1,
      "index.number_of_replicas": 1
    }
  }
}

PUT /_component_template/product_mappings
{
  "template": {
    "mappings": {
      "properties": {
        "name": {"type": "text"},
        "price": {"type": "double"},
        "timestamp": {"type": "date"}
      }
    }
  }
}

PUT /_index_template/products_composed
{
  "index_patterns": ["products-v2-*"],
  "composed_of": ["product_settings", "product_mappings"],
  "priority": 200
}

# View existing templates
GET /_index_template

# View specific template
GET /_index_template/product_template

# Delete template
DELETE /_index_template/product_template

# INDEX ALIASES - Create flexible references to indices
# ====================================================

# Example 3: Create basic aliases
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "ecommerce",
        "alias": "products"
      }
    },
    {
      "add": {
        "index": "ecommerce",
        "alias": "live_products"
      }
    }
  ]
}

# Example 4: Filtered alias (only in-stock products)
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "ecommerce",
        "alias": "in_stock_products",
        "filter": {
          "term": {
            "in_stock": true
          }
        }
      }
    }
  ]
}

# Example 5: Multiple index alias with routing
POST /_aliases
{
  "actions": [
    {
      "add": {
        "indices": ["ecommerce", "user_activity"],
        "alias": "analytics_data"
      }
    }
  ]
}

# Example 6: Atomic alias switching (Blue-Green deployment)
POST /_aliases
{
  "actions": [
    {
      "remove": {
        "index": "products_v1",
        "alias": "products"
      }
    },
    {
      "add": {
        "index": "products_v2",
        "alias": "products"
      }
    }
  ]
}

# View aliases
GET /_alias
GET /ecommerce/_alias
GET /_alias/products

# REINDEXING - Copy data between indices
# =====================================

# Example 7: Basic reindex operation
POST /_reindex
{
  "source": {
    "index": "ecommerce"
  },
  "dest": {
    "index": "ecommerce_backup"
  }
}

# Example 8: Reindex with query filter
POST /_reindex
{
  "source": {
    "index": "ecommerce",
    "query": {
      "range": {
        "price": {
          "gte": 1000
        }
      }
    }
  },
  "dest": {
    "index": "premium_products"
  }
}

# Example 9: Reindex with field transformation
POST /_reindex
{
  "source": {
    "index": "ecommerce"
  },
  "dest": {
    "index": "ecommerce_transformed"
  },
  "script": {
    "source": "ctx._source.price_category = ctx._source.price > 1500 ? 'premium' : 'standard'"
  }
}

# Example 10: Reindex from remote cluster
POST /_reindex
{
  "source": {
    "remote": {
      "host": "https://remote-cluster:9200",
      "username": "elastic",
      "password": "password"
    },
    "index": "remote_products"
  },
  "dest": {
    "index": "local_products"
  }
}

# Example 11: Reindex with version conflicts handling
POST /_reindex
{
  "conflicts": "proceed",
  "source": {
    "index": "ecommerce"
  },
  "dest": {
    "index": "ecommerce_copy",
    "version_type": "external"
  }
}

# Check reindex task progress
GET /_tasks?detailed=true&actions=*reindex

# PLUGIN MANAGEMENT
# ================

# Example 12: List installed plugins
GET /_cat/plugins?v

# Example 13: Node information with plugins
GET /_cat/nodes?h=name,version,plugins

# Install plugin (command line - not API)
# bin/elasticsearch-plugin install analysis-icu
# bin/elasticsearch-plugin install repository-s3

# Remove plugin (command line - not API)
# bin/elasticsearch-plugin remove analysis-icu

# BACKUP AND RESTORE (Snapshot/Restore)
# ====================================

# Example 14: Create snapshot repository (filesystem)
PUT /_snapshot/backup_repo
{
  "type": "fs",
  "settings": {
    "location": "/mount/backups/elasticsearch",
    "compress": true
  }
}

# Example 15: Create snapshot repository (S3)
PUT /_snapshot/s3_backup
{
  "type": "s3",
  "settings": {
    "bucket": "my-elasticsearch-backups",
    "region": "us-east-1",
    "compress": true,
    "server_side_encryption": true
  }
}

# Example 16: Create a snapshot
PUT /_snapshot/backup_repo/snapshot_1
{
  "indices": "ecommerce,user_activity",
  "ignore_unavailable": true,
  "include_global_state": false,
  "metadata": {
    "taken_by": "admin",
    "taken_because": "daily backup"
  }
}

# Example 17: Create snapshot with advanced options
PUT /_snapshot/backup_repo/full_backup
{
  "indices": "*",
  "ignore_unavailable": true,
  "include_global_state": true,
  "partial": false,
  "metadata": {
    "description": "Full cluster backup"
  }
}

# Example 18: List snapshots
GET /_snapshot/backup_repo/_all

# Example 19: Get snapshot information
GET /_snapshot/backup_repo/snapshot_1

# Example 20: Restore from snapshot
POST /_snapshot/backup_repo/snapshot_1/_restore
{
  "indices": "ecommerce",
  "ignore_unavailable": true,
  "include_global_state": false,
  "rename_pattern": "(.+)",
  "rename_replacement": "restored_$1"
}

# Example 21: Partial restore with index settings
POST /_snapshot/backup_repo/snapshot_1/_restore
{
  "indices": "ecommerce",
  "index_settings": {
    "index.number_of_replicas": 0
  },
  "ignore_index_settings": [
    "index.refresh_interval"
  ]
}

# Monitor snapshot progress
GET /_snapshot/backup_repo/snapshot_1/_status

# Delete snapshot
DELETE /_snapshot/backup_repo/snapshot_1

# Delete repository
DELETE /_snapshot/backup_repo

# Key Points:
# - Index templates: Define structure for new indices matching patterns
# - Component templates: Reusable building blocks for index templates
# - Aliases: Flexible references to indices, support filtering and routing
# - Reindexing: Copy/transform data between indices, supports remote clusters
# - Plugins: Extend Elasticsearch functionality (install via command line)
# - Snapshots: Point-in-time backups, support multiple repository types
# - Restore operations: Flexible recovery with renaming and setting overrides