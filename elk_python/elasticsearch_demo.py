
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import json
import random

# 1. Connect to Elasticsearch
def connect_to_elasticsearch():
    """
    Connect to Elasticsearch cluster
    """
    # For local development
    es = Elasticsearch(
        hosts=[{"host": "localhost", "port": 9200, "scheme": "http"}],
        # For production with authentication:
        # http_auth=('username', 'password'),
        # use_ssl=True,
        # verify_certs=True,
        # ca_certs='/path/to/ca.crt',
        timeout=30,
        max_retries=10,
        retry_on_timeout=True
    )

    # Test connection
    if es.ping():
        print("Connected to Elasticsearch successfully!")
        print(f"Cluster info: {es.info()['cluster_name']}")
    else:
        raise ConnectionError("Could not connect to Elasticsearch")

    return es

# 2. Create index with mapping (skip if exists)
def create_index_if_not_exists(es, index_name):
    """
    Create an index with proper mapping if it doesn't exist
    """
    if es.indices.exists(index=index_name):
        print(f"Index '{index_name}' already exists, skipping creation")
        return False

    # Define index mapping
    mapping = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "user_id": {"type": "keyword"},
                "user_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "action": {"type": "keyword"},
                "department": {"type": "keyword"},
                "status": {"type": "keyword"},
                "response_time": {"type": "float"},
                "ip_address": {"type": "ip"},
                "user_agent": {"type": "text"},
                "location": {
                    "type": "geo_point"
                },
                "session_duration": {"type": "integer"}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        }
    }

    # Create the index
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully")
    return True

# 3. Generate sample data
def generate_sample_data(num_records=100):
    """
    Generate sample user activity data
    """
    users = [
        {"id": "U001", "name": "John Doe", "dept": "engineering"},
        {"id": "U002", "name": "Jane Smith", "dept": "marketing"},
        {"id": "U003", "name": "Bob Johnson", "dept": "sales"},
        {"id": "U004", "name": "Alice Brown", "dept": "engineering"},
        {"id": "U005", "name": "Charlie Wilson", "dept": "hr"}
    ]

    actions = ["login", "logout", "file_upload", "file_download", "api_call", "database_query"]
    statuses = ["success", "failed", "timeout"]
    locations = [
        {"lat": 40.7128, "lon": -74.0060},  # New York
        {"lat": 37.7749, "lon": -122.4194}, # San Francisco
        {"lat": 51.5074, "lon": -0.1278},   # London
        {"lat": 35.6762, "lon": 139.6503},  # Tokyo
        {"lat": 19.0760, "lon": 72.8777}    # Mumbai
    ]

    sample_data = []
    base_time = datetime.now() - timedelta(days=7)

    for i in range(num_records):
        user = random.choice(users)
        location = random.choice(locations)

        record = {
            "timestamp": (base_time + timedelta(
                days=random.randint(0, 7),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).isoformat(),
            "user_id": user["id"],
            "user_name": user["name"],
            "action": random.choice(actions),
            "department": user["dept"],
            "status": random.choice(statuses),
            "response_time": round(random.uniform(0.1, 5.0), 2),
            "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "location": location,
            "session_duration": random.randint(60, 3600)  # 1 minute to 1 hour in seconds
        }
        sample_data.append(record)

    return sample_data

# 4. Insert data into Elasticsearch
def insert_data(es, index_name, data):
    """
    Bulk insert data into Elasticsearch
    """
    from elasticsearch.helpers import bulk

    # Prepare bulk data
    actions = []
    for doc in data:
        action = {
            "_index": index_name,
            "_source": doc
        }
        actions.append(action)

    # Perform bulk insert
    try:
        success, failed = bulk(es, actions)
        print(f"Successfully inserted {success} documents")
        if failed:
            print(f"Failed to insert {len(failed)} documents")
    except Exception as e:
        print(f"Error during bulk insert: {e}")

# 5. Search data
def search_data(es, index_name):
    """
    Perform various search queries
    """
    print("\n=== SEARCH OPERATIONS ===")

    # Simple match all query
    print("1. Match all documents (first 5):")
    response = es.search(
        index=index_name,
        body={
            "query": {"match_all": {}},
            "size": 5,
            "sort": [{"timestamp": {"order": "desc"}}]
        }
    )

    for hit in response['hits']['hits']:
        source = hit['_source']
        print(f"   {source['timestamp'][:19]} | {source['user_name']} | {source['action']} | {source['status']}")

    # Search for specific user
    print("\n2. Search for engineering department users:")
    response = es.search(
        index=index_name,
        body={
            "query": {
                "term": {"department": "engineering"}
            },
            "size": 3
        }
    )

    print(f"   Found {response['hits']['total']['value']} engineering users")
    for hit in response['hits']['hits']:
        source = hit['_source']
        print(f"   {source['user_name']} - {source['action']}")

    # Search for failed actions in last 24 hours
    print("\n3. Failed actions:")
    response = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [
                        {"term": {"status": "failed"}}
                    ]
                }
            },
            "size": 5
        }
    )

    print(f"   Found {response['hits']['total']['value']} failed actions")
    for hit in response['hits']['hits']:
        source = hit['_source']
        print(f"   {source['user_name']} - {source['action']} - {source['response_time']}ms")

# 6. Perform aggregations
def perform_aggregations(es, index_name):
    """
    Perform various aggregations on the data
    """
    print("\n=== AGGREGATION OPERATIONS ===")

    # 1. Count by department
    print("1. Count by Department:")
    response = es.search(
        index=index_name,
        body={
            "size": 0,
            "aggs": {
                "by_department": {
                    "terms": {"field": "department"}
                }
            }
        }
    )

    for bucket in response['aggregations']['by_department']['buckets']:
        print(f"   {bucket['key']}: {bucket['doc_count']} actions")

    # 2. Average response time by action type
    print("\n2. Average Response Time by Action:")
    response = es.search(
        index=index_name,
        body={
            "size": 0,
            "aggs": {
                "by_action": {
                    "terms": {"field": "action"},
                    "aggs": {
                        "avg_response_time": {
                            "avg": {"field": "response_time"}
                        }
                    }
                }
            }
        }
    )

    for bucket in response['aggregations']['by_action']['buckets']:
        avg_time = bucket['avg_response_time']['value']
        print(f"   {bucket['key']}: {avg_time:.2f}ms average ({bucket['doc_count']} samples)")

    # 3. Status distribution
    print("\n3. Status Distribution:")
    response = es.search(
        index=index_name,
        body={
            "size": 0,
            "aggs": {
                "status_distribution": {
                    "terms": {"field": "status"}
                }
            }
        }
    )

    total_docs = sum([bucket['doc_count'] for bucket in response['aggregations']['status_distribution']['buckets']])
    for bucket in response['aggregations']['status_distribution']['buckets']:
        percentage = (bucket['doc_count'] / total_docs) * 100
        print(f"   {bucket['key']}: {bucket['doc_count']} ({percentage:.1f}%)")

    # 4. Daily activity histogram
    print("\n4. Daily Activity Histogram:")
    response = es.search(
        index=index_name,
        body={
            "size": 0,
            "aggs": {
                "daily_activity": {
                    "date_histogram": {
                        "field": "timestamp",
                        "calendar_interval": "day",
                        "format": "yyyy-MM-dd"
                    }
                }
            }
        }
    )

    for bucket in response['aggregations']['daily_activity']['buckets']:
        print(f"   {bucket['key_as_string']}: {bucket['doc_count']} activities")

    # 5. Top users by session duration
    print("\n5. Top Users by Total Session Duration:")
    response = es.search(
        index=index_name,
        body={
            "size": 0,
            "aggs": {
                "top_users": {
                    "terms": {"field": "user_name.keyword"},
                    "aggs": {
                        "total_session_time": {
                            "sum": {"field": "session_duration"}
                        }
                    }
                }
            }
        }
    )

    for bucket in response['aggregations']['top_users']['buckets']:
        total_time = bucket['total_session_time']['value']
        hours = total_time / 3600
        print(f"   {bucket['key']}: {hours:.1f} hours total ({bucket['doc_count']} sessions)")

# Main execution function
def main():
    """
    Main function to demonstrate all operations
    """
    INDEX_NAME = "user_activity_logs"

    try:
        # Connect to Elasticsearch
        es = connect_to_elasticsearch()

        # Create index if not exists
        create_index_if_not_exists(es, INDEX_NAME)

        # Generate and insert sample data
        print("\nGenerating sample data...")
        sample_data = generate_sample_data(150)  # Generate 150 records

        print("Inserting data into Elasticsearch...")
        insert_data(es, INDEX_NAME, sample_data)

        # Wait for data to be indexed
        es.indices.refresh(index=INDEX_NAME)

        # Perform searches
        search_data(es, INDEX_NAME)

        # Perform aggregations
        perform_aggregations(es, INDEX_NAME)

        print("\n=== ELASTICSEARCH OPERATIONS COMPLETED SUCCESSFULLY ===")

    except Exception as e:
        print(f"Error: {e}")

# Run the main function
if __name__ == "__main__":
    main()
