# =============================================================================
# AGGREGATIONS - RANGE
# =============================================================================
# Range aggregation creates buckets for numeric ranges
# Each bucket contains documents with field values within the specified range

# Example 1: Price ranges for products
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "price_ranges": {
      "range": {
        "field": "price",
        "ranges": [
          {"to": 500},
          {"from": 500, "to": 1000},
          {"from": 1000, "to": 2000},
          {"from": 2000}
        ]
      },
      "aggs": {
        "avg_rating": {
          "avg": {
            "field": "rating"
          }
        }
      }
    }
  }
}

# Example 2: Custom keyed price ranges
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "price_segments": {
      "range": {
        "field": "price",
        "keyed": true,
        "ranges": [
          {"key": "budget", "to": 500},
          {"key": "mid_range", "from": 500, "to": 1500},
          {"key": "premium", "from": 1500, "to": 3000},
          {"key": "luxury", "from": 3000}
        ]
      }
    }
  }
}

# Example 3: Rating ranges to understand quality distribution
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "quality_ranges": {
      "range": {
        "field": "rating",
        "ranges": [
          {"key": "poor", "to": 3.0},
          {"key": "average", "from": 3.0, "to": 4.0},
          {"key": "good", "from": 4.0, "to": 4.5},
          {"key": "excellent", "from": 4.5}
        ]
      },
      "aggs": {
        "total_sales": {
          "sum": {
            "field": "sales_count"
          }
        }
      }
    }
  }
}

# Example 4: Sales performance ranges
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "sales_performance": {
      "range": {
        "field": "sales_count",
        "ranges": [
          {"key": "slow_moving", "to": 50},
          {"key": "moderate", "from": 50, "to": 100},
          {"key": "popular", "from": 100, "to": 200},
          {"key": "bestseller", "from": 200}
        ]
      },
      "aggs": {
        "brands": {
          "terms": {
            "field": "brand"
          }
        }
      }
    }
  }
}

# Example 5: Discount percentage ranges
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "discount_levels": {
      "range": {
        "field": "discount_percentage",
        "ranges": [
          {"key": "no_discount", "from": 0, "to": 0.1},
          {"key": "small_discount", "from": 0.1, "to": 10},
          {"key": "moderate_discount", "from": 10, "to": 20},
          {"key": "high_discount", "from": 20}
        ]
      },
      "aggs": {
        "avg_sales": {
          "avg": {
            "field": "sales_count"
          }
        }
      }
    }
  }
}

# Example 6: User session duration ranges
GET /user_activity/_search
{
  "size": 0,
  "aggs": {
    "session_duration_ranges": {
      "range": {
        "field": "duration_minutes",
        "ranges": [
          {"key": "quick_visit", "to": 2},
          {"key": "short_session", "from": 2, "to": 5},
          {"key": "medium_session", "from": 5, "to": 15},
          {"key": "long_session", "from": 15}
        ]
      },
      "aggs": {
        "avg_page_views": {
          "avg": {
            "field": "page_views"
          }
        }
      }
    }
  }
}

# Example 7: Page view ranges for engagement analysis
GET /user_activity/_search
{
  "size": 0,
  "aggs": {
    "engagement_levels": {
      "range": {
        "field": "page_views",
        "ranges": [
          {"key": "low_engagement", "to": 3},
          {"key": "medium_engagement", "from": 3, "to": 7},
          {"key": "high_engagement", "from": 7, "to": 15},
          {"key": "very_high_engagement", "from": 15}
        ]
      },
      "aggs": {
        "activity_types": {
          "terms": {
            "field": "activity_type"
          }
        }
      }
    }
  }
}

# Example 8: Multi-level range aggregation with filters
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        {"term": {"in_stock": true}}
      ]
    }
  },
  "aggs": {
    "price_ranges_by_category": {
      "terms": {
        "field": "category"
      },
      "aggs": {
        "price_distribution": {
          "range": {
            "field": "price",
            "ranges": [
              {"key": "low", "to": 500},
              {"key": "medium", "from": 500, "to": 1500},
              {"key": "high", "from": 1500}
            ]
          },
          "aggs": {
            "avg_rating": {
              "avg": {
                "field": "rating"
              }
            }
          }
        }
      }
    }
  }
}

# Example 9: Range with script field
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "value_score_ranges": {
      "range": {
        "script": {
          "source": "doc['rating'].value * doc['sales_count'].value / (doc['price'].value / 100)"
        },
        "ranges": [
          {"key": "poor_value", "to": 10},
          {"key": "average_value", "from": 10, "to": 50},
          {"key": "good_value", "from": 50, "to": 100},
          {"key": "excellent_value", "from": 100}
        ]
      }
    }
  }
}

# Key Points:
# - Range aggregation creates buckets based on numeric ranges
# - Use "from" for lower bound (inclusive), "to" for upper bound (exclusive)
# - "key" parameter provides custom names for buckets
# - "keyed" parameter returns results as a map instead of array
# - Ranges can be open-ended (only "from" or only "to")
# - Can use script fields for calculated ranges
# - Combine with sub-aggregations for deeper analysis