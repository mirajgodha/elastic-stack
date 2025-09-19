package com.example.elasticsearchdemo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.apache.http.util.EntityUtils;
import org.apache.http.HttpHost;

import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * ElasticsearchDemo - migrated to Elasticsearch 8.x Java client (elasticsearch-java) but
 * using the low-level RestClient for index creation and aggregation queries to keep mapping
 * and aggregation JSON simple and identical to the original implementation.
 *
 * It preserves the original feature set: index creation with mapping, sample data generation,
 * bulk inserts, search examples, and several aggregations.
 */
public class ElasticsearchDemo {
    private static final String INDEX_NAME = "user_activity_logs";
    private final RestClient restClient;
    private final ElasticsearchClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    public ElasticsearchDemo(String host, int port, boolean useSsl) {
        String scheme = useSsl ? "https" : "http";
        this.restClient = RestClient.builder(new HttpHost(host, port, scheme)).build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
    }

    /**
     * Create index with mapping if it does not exist. Uses low-level REST calls to allow
     * easy JSON mapping payloads.
     */
    public void createIndexIfNotExists() throws IOException {
        try {
            Request head = new Request("HEAD", "/" + INDEX_NAME);
            Response headResp = restClient.performRequest(head);
            if (headResp.getStatusLine().getStatusCode() == 200) {
                System.out.println("📋 Index '" + INDEX_NAME + "' already exists, skipping creation");
                return;
            }
        } catch (ResponseException re) {
            // 404 means index doesn't exist - continue to create
            if (re.getResponse().getStatusLine().getStatusCode() != 404) {
                throw re;
            }
        }

        String mappingJson = "{" +
                "\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":1}," +
                "\"mappings\":{\"properties\":{" +
                "\"timestamp\":{\"type\":\"date\",\"format\":\"strict_date_optional_time||epoch_millis\"}," +
                "\"user_name\":{\"type\":\"keyword\"}," +
                "\"department\":{\"type\":\"keyword\"}," +
                "\"action\":{\"type\":\"keyword\"}," +
                "\"status\":{\"type\":\"keyword\"}," +
                "\"response_time\":{\"type\":\"float\"}," +
                "\"ip_address\":{\"type\":\"ip\"}," +
                "\"session_duration\":{\"type\":\"integer\"}," +
                "\"location\":{\"type\":\"geo_point\"}" +
                "}}}"
                ;

        Request put = new Request("PUT", "/" + INDEX_NAME);
        put.setJsonEntity(mappingJson);
        Response putResp = restClient.performRequest(put);
        int status = putResp.getStatusLine().getStatusCode();
        if (status >= 200 && status < 300) {
            System.out.println("🎯 Index '" + INDEX_NAME + "' created successfully with mapping");
        } else {
            throw new IOException("Failed to create index, status=" + status + " body=" + EntityUtils.toString(putResp.getEntity()));
        }
    }

    /**
     * Generate sample user activity data similar to the original file
     */
    public List<Map<String, Object>> generateSampleData(int numRecords) {
        List<Map<String, Object>> sampleData = new ArrayList<>();
        Random random = new Random();

        double[][] locations = {
                {40.7128, -74.0060},  // New York
                {37.7749, -122.4194}, // San Francisco
                {51.5074, -0.1278},   // London
                {35.6762, 139.6503},  // Tokyo
                {19.0760, 72.8777}    // Mumbai
        };

        LocalDateTime baseTime = LocalDateTime.now().minusDays(7);
        DateTimeFormatter fmt = DateTimeFormatter.ISO_DATE_TIME;

        String[] departments = {"engineering", "sales", "hr", "support"};
        String[] actions = {"login", "logout", "view", "click", "purchase"};
        String[] statuses = {"success", "failure"};

        for (int i = 0; i < numRecords; i++) {
            Map<String, Object> record = new HashMap<>();
            String user = "user_" + (random.nextInt(50) + 1);
            String dept = departments[random.nextInt(departments.length)];
            String action = actions[random.nextInt(actions.length)];
            String status = statuses[random.nextInt(statuses.length)];

            LocalDateTime ts = baseTime.plusSeconds(random.nextInt(7 * 24 * 3600));

            record.put("user_name", user);
            record.put("department", dept);
            record.put("action", action);
            record.put("status", status);
            record.put("response_time", 50 + random.nextDouble() * 200); // ms
            record.put("ip_address", "192.168." + random.nextInt(256) + "." + random.nextInt(256));
            record.put("session_duration", random.nextInt(3600));
            double[] location = locations[random.nextInt(locations.length)];
            Map<String, Double> geoPoint = new HashMap<>();
            geoPoint.put("lat", location[0]);
            geoPoint.put("lon", location[1]);
            record.put("location", geoPoint);
            record.put("timestamp", ts.format(fmt));

            sampleData.add(record);
        }

        return sampleData;
    }

    /**
     * Bulk insert data using the java client bulk API
     */
    public void insertData(List<Map<String, Object>> data) throws IOException {
        List<BulkOperation> ops = new ArrayList<>();
        for (Map<String, Object> doc : data) {
            ops.add(BulkOperation.of(b -> b.index(i -> i.index(INDEX_NAME).document(doc))));
        }

        BulkRequest bulkRequest = BulkRequest.of(b -> b.operations(ops));
        BulkResponse bulkResp = client.bulk(bulkRequest);

        if (bulkResp.errors()) {
            System.err.println("⚠️  Some documents failed to index. Inspect items for details.");
            bulkResp.items().forEach(item -> {
                if (item.error() != null) {
                    System.err.println(item.error().reason());
                }
            });
        } else {
            System.out.println("📄 Successfully inserted " + data.size() + " documents");
        }

        // Refresh index so documents are immediately searchable
        Request refresh = new Request("POST", "/" + INDEX_NAME + "/_refresh");
        restClient.performRequest(refresh);
    }

    /**
     * Demonstrates several search scenarios using the Java client
     */
    public void performSearchOperations() throws IOException {
        System.out.println("\n=== 🔍 SEARCH OPERATIONS ===");

        // 1. Match all query with sorting (recent 5)
        System.out.println("1. Recent Activity (Last 5 entries):");
        SearchResponse<Map> resp = client.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.matchAll(m -> m))
                .size(5)
                .sort(st -> st.field(f -> f.field("timestamp").order(co.elastic.clients.elasticsearch._types.SortOrder.Desc))),
            Map.class);

        for (Hit<Map> hit : resp.hits().hits()) {
            Map src = hit.source();
            System.out.printf("   %s | %s | %s | %s%n",
                    src.get("timestamp"), src.get("user_name"), src.get("action"), src.get("status"));
        }

        // 2. Find successful engineering logins
        System.out.println("\n2. Successful engineering logins:");
        SearchResponse<Map> resp2 = client.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.bool(b -> b
                    .must(m1 -> m1.term(t -> t.field("department").value("engineering")))
                    .must(m2 -> m2.term(t -> t.field("action").value("login")))
                    .must(m3 -> m3.term(t -> t.field("status").value("success")))))
                .size(10),
            Map.class);

        System.out.println("   Found " + resp2.hits().total().value() + " successful engineering logins");
        for (Hit<Map> hit : resp2.hits().hits()) {
            Map src = hit.source();
            String ts = String.valueOf(src.get("timestamp"));
            System.out.printf("   %s at %s%n", src.get("user_name"), ts.length() > 19 ? ts.substring(0, 19) : ts);
        }
    }

    /**
     * Perform aggregations (terms, avg sub-agg, date histogram) using low-level JSON and parsing
     */
    public void performAggregations() throws IOException {
        System.out.println("\n=== 📊 AGGREGATION OPERATIONS ===");

        String aggQuery = "{\"size\":0,\"aggs\":{" +
            "\"by_department\":{\"terms\":{\"field\":\"department\"}}," +
            "\"by_action\":{\"terms\":{\"field\":\"action\",\"size\":10},\"aggs\":{\"avg_response_time\":{\"avg\":{\"field\":\"response_time\"}}}}," +
            "\"activity_over_time\":{\"date_histogram\":{\"field\":\"timestamp\",\"calendar_interval\":\"day\"}}" +
            "}}";

        Request req = new Request("GET", "/" + INDEX_NAME + "/_search");
        req.setJsonEntity(aggQuery);

        Response response = restClient.performRequest(req);
        String body = EntityUtils.toString(response.getEntity());
        Map<String, Object> parsed = mapper.readValue(body, Map.class);

        Map<String, Object> aggs = (Map<String, Object>) parsed.get("aggregations");

        // 1. by_department
        System.out.println("1. Activity Count by Department:");
        Map<String, Object> byDept = (Map<String, Object>) aggs.get("by_department");
        List<Map<String, Object>> deptBuckets = (List<Map<String, Object>>) byDept.get("buckets");
        for (Map<String, Object> b : deptBuckets) {
            System.out.printf("   %s: %d%n", b.get("key"), ((Number)b.get("doc_count")).longValue());
        }

        // 2. by_action with avg_response_time
        System.out.println("\n2. Action avg response time:");
        Map<String, Object> byAction = (Map<String, Object>) aggs.get("by_action");
        List<Map<String, Object>> actionBuckets = (List<Map<String, Object>>) byAction.get("buckets");
        for (Map<String, Object> b : actionBuckets) {
            Map<String, Object> avgAgg = (Map<String, Object>) b.get("avg_response_time");
            double avg = avgAgg.get("value") == null ? 
                Double.NaN : ((Number)avgAgg.get("value")).doubleValue();
            System.out.printf("   %s: avg=%.2fms (%d samples)%n", 
                b.get("key"), avg, ((Number)b.get("doc_count")).longValue());
        }

        // 3. activity_over_time (date buckets)
        System.out.println("\n3. Activity over time (daily):");
        Map<String, Object> overTime = (Map<String, Object>) aggs.get("activity_over_time");
        List<Map<String, Object>> timeBuckets = (List<Map<String, Object>>) overTime.get("buckets");
        for (Map<String, Object> b : timeBuckets) {
            System.out.printf("   %s -> %d%n", b.get("key_as_string"), ((Number)b.get("doc_count")).longValue());
        }
    }

    public void closeClient() throws IOException {
        if (restClient != null) {
            restClient.close();
            System.out.println("\n✅ Elasticsearch client closed successfully");
        }
    }

    // Small runnable demo main (keeps original flow)
    public static void main(String[] args) {
        ElasticsearchDemo demo = new ElasticsearchDemo("localhost", 9200, false);
        try {
            System.out.println("\n📝 Generating sample data...");
            List<Map<String, Object>> sampleData = demo.generateSampleData(200);

            System.out.println("💾 Creating index if needed...");
            demo.createIndexIfNotExists();

            System.out.println("💾 Inserting data into Elasticsearch...");
            demo.insertData(sampleData);

            demo.performSearchOperations();

            demo.performAggregations();

            System.out.println("\n🎉 All Elasticsearch operations completed successfully!");

        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                demo.closeClient();
            } catch (IOException e) {
                System.err.println("Error closing client: " + e.getMessage());
            }
        }
    }
}
