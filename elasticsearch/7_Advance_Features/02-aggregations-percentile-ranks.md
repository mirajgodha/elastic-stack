# =============================================================================
# AGGREGATIONS - PERCENTILE RANKS
# =============================================================================
# Percentile ranks show what percentage of values are below a given value
# Useful for understanding distribution and relative positioning

# Example 1: Find what percentile rank different prices fall into
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "price_percentile_ranks": {
      "percentile_ranks": {
        "field": "price",
        "values": [500, 1000, 1500, 2000]
      }
    }
  }
}

# Example 2: Percentile ranks for product ratings
# This tells us what percentage of products have ratings below specified values
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "rating_percentile_ranks": {
      "percentile_ranks": {
        "field": "rating",
        "values": [4.0, 4.5, 4.8]
      }
    }
  }
}

# Example 3: Percentile ranks with filter
# Find percentile ranks for laptop prices only
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "term": {
      "category": "laptops"
    }
  },
  "aggs": {
    "laptop_price_percentile_ranks": {
      "percentile_ranks": {
        "field": "price",
        "values": [1200, 1500, 2000, 2500]
      }
    }
  }
}

# Example 4: Multiple percentile ranks with custom keyed output
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "sales_percentile_ranks": {
      "percentile_ranks": {
        "field": "sales_count",
        "values": [50, 100, 150, 200],
        "keyed": true
      }
    }
  }
}

# What this means:
# - If a value shows "50.0", it means 50% of documents have values below this threshold
# - Higher percentile rank = value is above more documents in the dataset
# - Lower percentile rank = value is below more documents in the dataset