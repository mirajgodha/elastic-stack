# =============================================================================
# AGGREGATIONS - GEOHASH GRID
# =============================================================================
# Geohash grid aggregation groups geo_point values into geographic grid cells
# Useful for geographic clustering and mapping applications

# First, let's add some more geographic data for better demos
POST /_bulk
{ "index": { "_index": "ecommerce", "_id": "geo1" }}
{ "product_id": "GEO001", "name": "Product SF", "location": { "lat": 37.7749, "lon": -122.4194 }, "price": 1000, "category": "test" }
{ "index": { "_index": "ecommerce", "_id": "geo2" }}
{ "product_id": "GEO002", "name": "Product NYC", "location": { "lat": 40.7128, "lon": -74.0060 }, "price": 1200, "category": "test" }
{ "index": { "_index": "ecommerce", "_id": "geo3" }}
{ "product_id": "GEO003", "name": "Product LA", "location": { "lat": 34.0522, "lon": -118.2437 }, "price": 900, "category": "test" }
{ "index": { "_index": "ecommerce", "_id": "geo4" }}
{ "product_id": "GEO004", "name": "Product Chicago", "location": { "lat": 41.8781, "lon": -87.6298 }, "price": 1100, "category": "test" }

# Example 1: Basic geohash grid aggregation
# size = 0 , We don’t want actual documents, only aggregation results.
GET /ecommerce/_search
{
  "size": 0, 
  "aggs": {
    "geographic_clusters": {
      "geohash_grid": {
        "field": "location",
        "precision": 3
      }
    }
  }
}

# Example 2: Geohash grid with higher precision for detailed clustering
# if your data covers 500 geohash grid cells at precision 5, but you set "size": 10, ES will only return the top 10 cells (by document count).
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "detailed_clusters": {
      "geohash_grid": {
        "field": "location",
        "precision": 5,
        "size": 10
      },
      "aggs": {
        "avg_price": {
          "avg": {
            "field": "price"
          }
        }
      }
    }
  }
}

# Example 3: Geohash grid with bounding box filter
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "geo_bounding_box": {
      "location": {
        "top_left": {
          "lat": 45.0,
          "lon": -125.0
        },
        "bottom_right": {
          "lat": 30.0,
          "lon": -115.0
        }
      }
    }
  },
  "aggs": {
    "west_coast_clusters": {
      "geohash_grid": {
        "field": "location",
        "precision": 4
      }
    }
  }
}

# Example 4: Geohash grid with shards_size parameter for accuracy
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "accurate_clusters": {
      "geohash_grid": {
        "field": "location",
        "precision": 4,
        "size": 10,
        "shard_size": 100
      },
      "aggs": {
        "product_count": {
          "value_count": {
            "field": "product_id"
          }
        },
        "total_sales": {
          "sum": {
            "field": "sales_count"
          }
        }
      }
    }
  }
}

# Example 5: Multi-level geographic analysis
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "broad_regions": {
      "geohash_grid": {
        "field": "location",
        "precision": 2
      },
      "aggs": {
        "detailed_areas": {
          "geohash_grid": {
            "field": "location",
            "precision": 4
          }
        },
        "brand_distribution": {
          "terms": {
            "field": "brand"
          }
        }
      }
    }
  }
}

# Example 6: User activity geographic clustering
GET /user_activity/_search
{
  "size": 0,
  "aggs": {
    "user_geo_clusters": {
      "geohash_grid": {
        "field": "location",
        "precision": 3
      },
      "aggs": {
        "activity_breakdown": {
          "terms": {
            "field": "activity_type"
          }
        },
        "avg_session_duration": {
          "avg": {
            "field": "duration_minutes"
          }
        }
      }
    }
  }
}

# Example 7: Geographic clustering with bounds
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "bounded_clusters": {
      "geohash_grid": {
        "field": "location",
        "precision": 3,
        "bounds": {
          "top_left": {
            "lat": 50.0,
            "lon": -130.0
          },
          "bottom_right": {
            "lat": 20.0,
            "lon": -60.0
          }
        }
      }
    }
  }
}

# Example 8: Complex geographic analysis with filters
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        {"range": {"price": {"gte": 500}}},
        {"term": {"in_stock": true}}
      ]
    }
  },
  "aggs": {
    "premium_product_clusters": {
      "geohash_grid": {
        "field": "location",
        "precision": 3
      },
      "aggs": {
        "avg_price": {
          "avg": {
            "field": "price"
          }
        },
        "max_rating": {
          "max": {
            "field": "rating"
          }
        },
        "top_product": {
          "top_hits": {
            "size": 1,
            "_source": ["name", "price", "rating"],
            "sort": [{"rating": {"order": "desc"}}]
          }
        }
      }
    }
  }
}

# Key Points:
# - Precision levels: 1 (lowest) to 12 (highest detail)
# - Each precision level roughly halves cell size
# - Higher precision = more detailed clustering but more cells
# - Use bounds to limit the geographic area
# - size controls maximum buckets returned
# - shard_size improves accuracy for distributed data