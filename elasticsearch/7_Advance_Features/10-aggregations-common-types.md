# =============================================================================
# AGGREGATIONS - TERMS, HISTOGRAM, DATE HISTOGRAM, STATS
# =============================================================================
# Multiple common aggregation types in one file for efficiency

# TERMS AGGREGATION - Groups documents by field values
# =====================================================

# Example 1: Most popular brands
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "popular_brands": {
      "terms": {
        "field": "brand",
        "size": 10,
        "order": {"_count": "desc"}
      }
    }
  }
}

# Example 2: Categories with sub-aggregations
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "categories": {
      "terms": {
        "field": "category"
      },
      "aggs": {
        "avg_price": {"avg": {"field": "price"}},
        "avg_rating": {"avg": {"field": "rating"}}
      }
    }
  }
}

# HISTOGRAM AGGREGATION - Creates buckets for numeric values
# =========================================================

# Example 3: Price histogram with 500 intervals
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "price_histogram": {
      "histogram": {
        "field": "price",
        "interval": 500,
        "min_doc_count": 1
      }
    }
  }
}

# Example 4: Rating histogram with custom intervals
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "rating_distribution": {
      "histogram": {
        "field": "rating",
        "interval": 0.5,
        "extended_bounds": {
          "min": 0,
          "max": 5
        }
      }
    }
  }
}

# DATE HISTOGRAM AGGREGATION - Time-based bucketing
# =================================================

# Example 5: Products created by month
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "products_over_time": {
      "date_histogram": {
        "field": "created_date",
        "calendar_interval": "month",
        "format": "yyyy-MM"
      },
      "aggs": {
        "avg_price": {"avg": {"field": "price"}}
      }
    }
  }
}

# Example 6: User activity by day
GET /user_activity/_search
{
  "size": 0,
  "aggs": {
    "daily_activity": {
      "date_histogram": {
        "field": "timestamp",
        "calendar_interval": "day",
        "format": "yyyy-MM-dd"
      },
      "aggs": {
        "activity_types": {
          "terms": {"field": "activity_type"}
        }
      }
    }
  }
}

# STATS AGGREGATION - Basic statistics
# ====================================

# Example 7: Price statistics
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "price_stats": {
      "stats": {
        "field": "price"
      }
    }
  }
}

# Example 8: Rating statistics by category
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "categories": {
      "terms": {"field": "category"},
      "aggs": {
        "rating_stats": {
          "stats": {"field": "rating"}
        }
      }
    }
  }
}

# EXTENDED STATS AGGREGATION - Advanced statistics
# ================================================

# Example 9: Extended price statistics
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "extended_price_stats": {
      "extended_stats": {
        "field": "price"
      }
    }
  }
}

# MIN/MAX AGGREGATIONS
# ===================

# Example 10: Price extremes
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "min_price": {"min": {"field": "price"}},
    "max_price": {"max": {"field": "price"}},
    "min_rating": {"min": {"field": "rating"}},
    "max_rating": {"max": {"field": "rating"}}
  }
}

# SUM AGGREGATION
# ==============

# Example 11: Total sales and revenue
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "total_sales_count": {
      "sum": {"field": "sales_count"}
    },
    "total_revenue": {
      "sum": {
        "script": {
          "source": "doc['price'].value * doc['sales_count'].value"
        }
      }
    }
  }
}