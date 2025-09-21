# =============================================================================
# AGGREGATIONS - FILTERS
# =============================================================================
# Filters aggregation applies multiple filters to create named buckets
# Each filter creates a bucket containing documents that match the filter

# Example 1: Products by price range
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "price_ranges": {
      "filters": {
        "filters": {
          "budget": {
            "range": {
              "price": {
                "lt": 500
              }
            }
          },
          "mid_range": {
            "range": {
              "price": {
                "gte": 500,
                "lt": 1500
              }
            }
          },
          "premium": {
            "range": {
              "price": {
                "gte": 1500
              }
            }
          }
        }
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

# Example 2: Products by availability and stock status
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "product_status": {
      "filters": {
        "filters": {
          "in_stock": {
            "term": {
              "in_stock": true
            }
          },
          "out_of_stock": {
            "term": {
              "in_stock": false
            }
          },
          "highly_rated": {
            "range": {
              "rating": {
                "gte": 4.5
              }
            }
          },
          "on_discount": {
            "range": {
              "discount_percentage": {
                "gt": 0
              }
            }
          }
        }
      }
    }
  }
}

# Example 3: User activity patterns
GET /user_activity/_search
{
  "size": 0,
  "aggs": {
    "activity_analysis": {
      "filters": {
        "filters": {
          "mobile_users": {
            "term": {
              "device_type": "mobile"
            }
          },
          "desktop_users": {
            "term": {
              "device_type": "desktop"
            }
          },
          "high_engagement": {
            "range": {
              "page_views": {
                "gte": 5
              }
            }
          },
          "quick_visits": {
            "range": {
              "duration_minutes": {
                "lt": 5
              }
            }
          }
        }
      },
      "aggs": {
        "avg_session_duration": {
          "avg": {
            "field": "duration_minutes"
          }
        }
      }
    }
  }
}

# Example 4: Complex filters with boolean queries
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "complex_segments": {
      "filters": {
        "filters": {
          "apple_premium": {
            "bool": {
              "must": [
                {"term": {"brand": "Apple"}},
                {"range": {"price": {"gte": 1000}}}
              ]
            }
          },
          "discounted_electronics": {
            "bool": {
              "must": [
                {"terms": {"category": ["laptops", "smartphones"]}},
                {"range": {"discount_percentage": {"gt": 10}}}
              ]
            }
          },
          "bestsellers": {
            "bool": {
              "must": [
                {"range": {"sales_count": {"gte": 100}}},
                {"range": {"rating": {"gte": 4.0}}}
              ]
            }
          }
        }
      },
      "aggs": {
        "total_revenue": {
          "sum": {
            "script": {
              "source": "doc['price'].value * doc['sales_count'].value"
            }
          }
        }
      }
    }
  }
}

# Example 5: Time-based filters (user activity)
GET /user_activity/_search
{
  "size": 0,
  "aggs": {
    "time_segments": {
      "filters": {
        "filters": {
          "morning_activity": {
            "script": {
              "source": "doc['timestamp'].value.hourOfDay >= 6 && doc['timestamp'].value.hourOfDay < 12"
            }
          },
          "afternoon_activity": {
            "script": {
              "source": "doc['timestamp'].value.hourOfDay >= 12 && doc['timestamp'].value.hourOfDay < 18"
            }
          },
          "evening_activity": {
            "script": {
              "source": "doc['timestamp'].value.hourOfDay >= 18 || doc['timestamp'].value.hourOfDay < 6"
            }
          }
        }
      }
    }
  }
}

# Example 6: Geographic filters
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "geographic_segments": {
      "filters": {
        "filters": {
          "west_coast": {
            "geo_bounding_box": {
              "location": {
                "top_left": {
                  "lat": 49.0,
                  "lon": -125.0
                },
                "bottom_right": {
                  "lat": 32.0,
                  "lon": -114.0
                }
              }
            }
          },
          "east_coast": {
            "geo_bounding_box": {
              "location": {
                "top_left": {
                  "lat": 45.0,
                  "lon": -82.0
                },
                "bottom_right": {
                  "lat": 25.0,
                  "lon": -67.0
                }
              }
            }
          },
          "central": {
            "geo_bounding_box": {
              "location": {
                "top_left": {
                  "lat": 49.0,
                  "lon": -114.0
                },
                "bottom_right": {
                  "lat": 25.0,
                  "lon": -82.0
                }
              }
            }
          }
        }
      }
    }
  }
}

# Example 7: Filters with other_bucket
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "brand_segments": {
      "filters": {
        "other_bucket": true,
        "other_bucket_key": "other_brands",
        "filters": {
          "apple": {
            "term": {
              "brand": "Apple"
            }
          },
          "samsung": {
            "term": {
              "brand": "Samsung"
            }
          }
        }
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

# Example 8: Nested filters with sub-aggregations
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "product_categories": {
      "filters": {
        "filters": {
          "laptops": {
            "term": {
              "category": "laptops"
            }
          },
          "smartphones": {
            "term": {
              "category": "smartphones"
            }
          }
        }
      },
      "aggs": {
        "brand_breakdown": {
          "terms": {
            "field": "brand"
          }
        },
        "price_stats": {
          "stats": {
            "field": "price"
          }
        }
      }
    }
  }
}

# Key Points:
# - Each filter creates a named bucket
# - Documents can appear in multiple buckets if they match multiple filters
# - Filters can be simple terms, ranges, or complex boolean queries
# - Sub-aggregations run on documents in each filter bucket
# - Use other_bucket to capture documents that don't match any filter