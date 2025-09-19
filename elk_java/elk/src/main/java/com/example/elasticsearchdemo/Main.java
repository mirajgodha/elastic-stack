package com.example.elasticsearchdemo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        ElasticsearchDemo demo = new ElasticsearchDemo("localhost", 9200, false);
        try {
            System.out.println("\n📝 Generating sample data...");
            List<Map<String, Object>> sampleData = demo.generateSampleData(200);

            System.out.println("💾 Creating index if needed...");
            demo.createIndexIfNotExists();

            System.out.println("💾 Inserting data into Elasticsearch...");
            demo.insertData(sampleData);

            System.out.println("\n🔍 Running search operations...");
            demo.performSearchOperations();

            System.out.println("\n📊 Running aggregations...");
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
