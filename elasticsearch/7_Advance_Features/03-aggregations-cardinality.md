# =============================================================================
# AGGREGATIONS - CARDINALITY
# =============================================================================
# Cardinality aggregation counts the number of unique/distinct values in a field
# Similar to SQL's COUNT(DISTINCT field_name)

# Example 1: Count unique brands in the product catalog
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "unique_brands": {
      "cardinality": {
        "field": "brand"
      }
    }
  }
}

# Example 2: Count unique categories
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "unique_categories": {
      "cardinality": {
        "field": "category"
      }
    }
  }
}

# Example 3: Count unique users who made purchases (using user_activity index)
GET /user_activity/_search
{
  "size": 0,
  "query": {
    "term": {
      "activity_type": "purchase"
    }
  },
  "aggs": {
    "unique_buyers": {
      "cardinality": {
        "field": "user_id"
      }
    }
  }
}

# Example 4: Cardinality with precision control
# Higher precision_threshold gives more accurate results but uses more memory
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "unique_products_precise": {
      "cardinality": {
        "field": "product_id",
        "precision_threshold": 1000
      }
    }
  }
}

# Example 5: Multiple cardinality aggregations
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "unique_brands": {
      "cardinality": {
        "field": "brand"
      }
    },
    "unique_categories": {
      "cardinality": {
        "field": "category"
      }
    },
    "products_in_stock": {
      "filter": {
        "term": {
          "in_stock": true
        }
      },
      "aggs": {
        "unique_in_stock_products": {
          "cardinality": {
            "field": "product_id"
          }
        }
      }
    }
  }
}

# Example 6: Cardinality by category (using terms aggregation + cardinality)
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "categories": {
      "terms": {
        "field": "category"
      },
      "aggs": {
        "unique_brands_per_category": {
          "cardinality": {
            "field": "brand"
          }
        }
      }
    }
  }
}

# Example 7: Cardinality on text field using script
# Count unique words in product descriptions
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "unique_description_words": {
      "cardinality": {
        "script": {
          "source": "doc['description.keyword'].value.split(' ').length"
        }
      }
    }
  }
}

# Notes:
# - Cardinality is approximate for large datasets (uses HyperLogLog algorithm)
# - precision_threshold controls accuracy vs memory usage
# - Default precision_threshold is 3000
# - Results are exact up to precision_threshold, then approximate