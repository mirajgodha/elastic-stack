# Steps to run

## Steps to compile using maven
```mvn clean package```

### Fat jar is compiled and cheked in
```java -jar target/elasticsearch-demo-1.0.0-SNAPSHOT.jar```

## Check aggregation on Kibana

```POST /_sql?format=txt
{
    "query": """
    SELECT department, count(*) FROM "user_activity_logs" group by department
    """
}
```
