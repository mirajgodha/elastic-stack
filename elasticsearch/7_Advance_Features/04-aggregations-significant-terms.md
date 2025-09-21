# =============================================================================
# AGGREGATIONS - SIGNIFICANT TERMS
# =============================================================================
# Significant terms find unusual or interesting terms that appear more frequently
# in the foreground set compared to the background set (entire index)

# Example 1: Find significant terms in product descriptions for high-rated products
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "range": {
      "rating": {
        "gte": 4.7
      }
    }
  },
  "aggs": {
    "significant_terms_high_rated": {
      "significant_terms": {
        "field": "description",
        "size": 10
      }
    }
  }
}

# Example 2: Significant terms in tags for premium products (price > 1500)
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "range": {
      "price": {
        "gte": 1500
      }
    }
  },
  "aggs": {
    "significant_tags_premium": {
      "significant_terms": {
        "field": "tags",
        "size": 5,
        "min_doc_count": 1
      }
    }
  }
}

# Example 3: Significant terms with custom background filter
# Find terms significant in Apple products compared to all products
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "term": {
      "brand": "Apple"
    }
  },
  "aggs": {
    "significant_apple_terms": {
      "significant_terms": {
        "field": "description",
        "background_filter": {
          "match_all": {}
        },
        "size": 8
      }
    }
  }
}

# Example 4: Significant terms with different significance heuristics
# Using mutual information instead of default JLH score
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "term": {
      "category": "laptops"
    }
  },
  "aggs": {
    "significant_laptop_terms": {
      "significant_terms": {
        "field": "description",
        "mutual_information": {},
        "size": 10,
        "min_doc_count": 1
      }
    }
  }
}

# Example 5: Significant terms with script field
# Analyze user agents for mobile users (from user_activity index)
GET /user_activity/_search
{
  "size": 0,
  "query": {
    "term": {
      "device_type": "mobile"
    }
  },
  "aggs": {
    "significant_mobile_terms": {
      "significant_terms": {
        "field": "user_agent",
        "size": 5
      }
    }
  }
}

# Example 6: Significant terms with percentage and chi-square scoring
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "sales_count": {
              "gte": 100
            }
          }
        }
      ]
    }
  },
  "aggs": {
    "significant_bestseller_terms": {
      "significant_terms": {
        "field": "tags",
        "chi_square": {},
        "percentage": {},
        "size": 10
      }
    }
  }
}

# Example 7: Nested significant terms analysis
# Find significant terms by category
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "categories": {
      "terms": {
        "field": "category"
      },
      "aggs": {
        "significant_category_terms": {
          "significant_terms": {
            "field": "description",
            "size": 3
          }
        }
      }
    }
  }
}

# Example 8: Significant terms with include/exclude patterns
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "range": {
      "rating": {
        "gte": 4.5
      }
    }
  },
  "aggs": {
    "significant_quality_terms": {
      "significant_terms": {
        "field": "description",
        "include": ".*quality.*|.*performance.*|.*premium.*",
        "size": 10
      }
    }
  }
}

# Explanation:
# - Significant terms help identify what makes a subset unique
# - Score indicates how "significant" or unusual the term is in the subset
# - Higher scores mean the term is much more common in subset vs entire dataset
# - Useful for anomaly detection, content recommendation, and feature analysis