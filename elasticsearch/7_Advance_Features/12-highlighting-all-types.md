# =============================================================================
# HIGHLIGHTING - DIFFERENT HIGHLIGHTER TYPES
# =============================================================================
# Elasticsearch provides multiple highlighting options to show matching text

# BASIC HIGHLIGHTER - Default plain highlighter
# =============================================

# Key Points:
# - Basic highlighter: Good for simple cases
# - FVH: Fast performance, requires term_vector: with_positions_offsets
# - Postings: Memory efficient, requires index_options: offsets  
# - Unified: Default in 8.x, good balance of speed and features
# - fragment_size: Characters per fragment (0 = entire field)
# - number_of_fragments: Max fragments to return
# - require_field_match: Only highlight if query matches that specific field
# - Custom pre/post tags for styling highlights



# Example 1: Basic highlighting in product descriptions
GET /ecommerce/_search
{
  "query": {
    "match": {
      "description": "advanced camera"
    }
  },
  "highlight": {
    "fields": {
      "description": {}
    }
  }
}

# Example 2: Highlighting with custom tags
GET /ecommerce/_search
{
  "query": {
    "match": {
      "name": "MacBook Pro"
    }
  },
  "highlight": {
    "pre_tags": ["<strong>"],
    "post_tags": ["</strong>"],
    "fields": {
      "name": {},
      "description": {}
    }
  }
}

# Example 3: Multiple field highlighting with different configurations
GET /ecommerce/_search
{
  "query": {
    "multi_match": {
      "query": "Apple smartphone smartwatch",
      "fields": ["name", "description", "brand"]
    }
  },
  "highlight": {
    "fields": {
      "name": {
        "pre_tags": ["<mark>"],
        "post_tags": ["</mark>"]
      },
      "description": {
        "fragment_size": 150,
        "number_of_fragments": 2
      },
      "brand": {}
    }
  }
}

# FAST VECTOR HIGHLIGHTER (FVH) - High performance highlighting
# =============================================================
# 🔹 fragment_size

# Defines the size (in characters) of each highlighted snippet.

# Here it’s 200 → ES will return text chunks of up to 200 characters around the match.

# Example 4: Using Fast Vector Highlighter
GET /ecommerce/_search
{
  "query": {
    "match": {
      "description": "processor performance"
    }
  },
  "highlight": {
    "type": "fvh",
    "fields": {
      "description": {
        "fragment_size": 100,
        "number_of_fragments": 3,
        "matched_fields": ["description"]
      }
    }
  }
}


PUT /ecommerce_v2
{
  "mappings": {
    "properties": {
      "product_id": { "type": "keyword" },
      "name": { "type": "text", "analyzer": "standard" },
      "description": { "type": "text" ,"term_vector": "with_positions_offsets"},
      "category": { "type": "keyword" },
      "brand": { "type": "keyword" },
      "price": { "type": "double" },
      "rating": { "type": "float" },
      "tags": { "type": "keyword" },
      "created_date": { "type": "date" },
      "location": { "type": "geo_point" },
      "in_stock": { "type": "boolean" },
      "sales_count": { "type": "integer" },
      "discount_percentage": { "type": "float" },
      "reviews": {
        "type": "nested",
        "properties": {
          "user": { "type": "keyword" },
          "rating": { "type": "integer" },
          "comment": { "type": "text" }
        }
      },
      "suggest": {
        "type": "completion",
        "analyzer": "simple",
        "preserve_separators": true,
        "preserve_position_increments": true,
        "max_input_length": 50,
        "contexts": [
          {
            "name": "category",
            "type": "category"
          }
        ]
      }
    }
  },
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "analysis": {
      "analyzer": {
        "custom_analyzer": {
          "type": "standard",
          "stopwords": "_english_"
        }
      }
    }
  }
}

# Reindex data from old index to new one:
POST _reindex
{
  "source": { "index": "ecommerce" },
  "dest":   { "index": "ecommerce_v2" }
}


GET /ecommerce_v2/_search
{
  "query": {
    "match": {
      "description": "processor performance"
    }
  },
  "highlight": {
    "type": "fvh",
    "fields": {
      "description": {
        "fragment_size": 100,
        "number_of_fragments": 3,
        "matched_fields": ["description"]
      }
    }
  }
}

# Example 5: FVH with phrase matching
# 🔹 phrase_limit

# Controls how many matching phrases (tokens) ES considers when building highlights.

# Default is 256.

# Bigger value = more phrases checked → can produce more highlight snippets, but uses more memory/CPU.
GET /ecommerce_v2/_search
{
  "query": {
    "match_phrase": {
      "description": "M2 chip"
    }
  },
  "highlight": {
    "type": "fvh",
    "fields": {
      "description": {
        "phrase_limit": 256,
        "fragment_size": 200
      }
    }
  }
}

# POSTINGS HIGHLIGHTER - Memory efficient highlighter
# ===================================================

# Example 6: Using Postings Highlighter (requires index_options: offsets)
# Note: This works best when the field is indexed with offsets
GET /ecommerce/_search
{
  "query": {
    "match": {
      "description": "professional design"
    }
  },
  "highlight": {
    "type": "postings",
    "fields": {
      "description": {
        "number_of_fragments": 2
      }
    }
  }
}

# UNIFIED HIGHLIGHTER - Default in Elasticsearch 8.x
# ==================================================

# Example 7: Unified highlighter with custom settings
GET /ecommerce/_search
{
  "query": {
    "bool": {
      "should": [
        {"match": {"name": "laptop"}},
        {"match": {"description": "portable"}}
      ]
    }
  },
  "highlight": {
    "type": "unified",
    "fields": {
      "name": {
        "highlight_query": {
          "match": {
            "name": "laptop"
          }
        }
      },
      "description": {
        "fragment_size": 150,
        "number_of_fragments": 1,
        "no_match_size": 50
      }
    }
  }
}

# Example 8: Highlighting with require_field_match
GET /ecommerce/_search
{
  "query": {
    "multi_match": {
      "query": "Apple iPhone",
      "fields": ["name", "brand", "description"]
    }
  },
  "highlight": {
    "require_field_match": true,
    "fields": {
      "name": {},
      "brand": {},
      "description": {}
    }
  }
}

# Example 9: Global highlighting settings with field overrides
GET /ecommerce/_search
{
  "query": {
    "match": {
      "description": "wireless connectivity"
    }
  },
  "highlight": {
    "pre_tags": ["<em class='highlight'>"],
    "post_tags": ["</em>"],
    "fragment_size": 100,
    "number_of_fragments": 2,
    "fields": {
      "description": {},
      "name": {
        "fragment_size": 0,
        "number_of_fragments": 0
      }
    }
  }
}

# Example 10: Highlighting with boundary scanner
GET /ecommerce/_search
{
  "query": {
    "match": {
      "description": "advanced features"
    }
  },
  "highlight": {
    "fields": {
      "description": {
        "type": "unified",
        "fragment_size": 150,
        "boundary_chars": ".,!? \\t\\n",
        "boundary_max_scan": 20
      }
    }
  }
}

# Example 11: Highlighting with encoder (HTML escaping)
GET /ecommerce/_search
{
  "query": {
    "match": {
      "description": "M1 & M2"
    }
  },
  "highlight": {
    "encoder": "html",
    "fields": {
      "description": {
        "pre_tags": ["<strong>"],
        "post_tags": ["</strong>"]
      }
    }
  }
}

# Example 12: Complex highlighting with multiple queries
GET /ecommerce/_search
{
  "query": {
    "bool": {
      "should": [
        {"match": {"name": {"query": "iPhone", "boost": 2}}},
        {"match": {"description": "smartphone"}},
        {"range": {"rating": {"gte": 4.5}}}
      ]
    }
  },
  "highlight": {
    "fields": {
      "name": {
        "highlight_query": {
          "match": {
            "name": "iPhone"
          }
        },
        "pre_tags": ["<strong class='name-highlight'>"],
        "post_tags": ["</strong>"]
      },
      "description": {
        "highlight_query": {
          "match": {
            "description": "smartphone"
          }
        },
        "fragment_size": 100,
        "number_of_fragments": 2
      }
    }
  }
}

