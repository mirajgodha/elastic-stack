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
# 🔹 What precision_threshold means
# cardinality uses a probabilistic algorithm (HyperLogLog++) → it’s approximate, not exact.
# precision_threshold controls how accurate the unique count should be.
#   - Low threshold (default 3000) → faster, less memory, but result may have a small error.
#   - Higher threshold → more accurate, but uses more memory.
#   - If the actual unique count ≤ threshold → the result is exact.
#   - If it’s much higher than threshold → result is approximate (but still close).
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
# This query counts the unique brands, unique categories, and unique in-stock products in the ecommerce index. ✅
#SELECT
#    COUNT(DISTINCT brand)        AS unique_brands,
#    COUNT(DISTINCT category)     AS unique_categories,
#    COUNT(DISTINCT CASE WHEN in_stock = TRUE THEN product_id END) AS unique_in_stock_products
#FROM ecommerce;

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
# This query groups products by category and, for each category, counts the number of unique brands. ✅
#SELECT
#    category,
#    COUNT(DISTINCT brand) AS unique_brands_per_category
#FROM ecommerce
#GROUP BY category;

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


# Notes:
# - Cardinality is approximate for large datasets (uses HyperLogLog algorithm)
# - precision_threshold controls accuracy vs memory usage
# - Default precision_threshold is 3000
# - Results are exact up to precision_threshold, then approximate