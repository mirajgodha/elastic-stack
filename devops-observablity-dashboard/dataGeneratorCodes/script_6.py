
# 7. OPENSHIFT/KUBERNETES LOGS
openshift_logs = []
namespaces = ['production', 'staging', 'development', 'monitoring']
resources = ['Pod', 'Deployment', 'Service', 'ConfigMap', 'Secret', 'Ingress']
events = ['Created', 'Updated', 'Deleted', 'Failed', 'Scaled', 'Restarted']

for i in range(50):
    log = {
        "timestamp": generate_timestamp(hours_ago=random.randint(0, 72)),
        "service": "openshift",
        "cluster": random.choice(['prod-cluster-1', 'staging-cluster', 'dev-cluster']),
        "namespace": random.choice(namespaces),
        "resource_type": random.choice(resources),
        "resource_name": f"{random.choice(['api', 'web', 'worker', 'db'])}-{random.randint(1, 10)}",
        "event": random.choice(events),
        "node": f"worker-node-{random.randint(1, 5)}",
        "cpu_usage_percent": round(random.uniform(10, 95), 2),
        "memory_usage_mb": random.randint(128, 4096),
        "restart_count": random.randint(0, 10),
        "status": random.choice(['Running', 'Pending', 'Failed', 'Succeeded', 'CrashLoopBackOff']),
        "log_level": random.choice(['INFO', 'WARN', 'ERROR', 'CRITICAL']),
        "message": f"{random.choice(resources)} {random.choice(events).lower()} in namespace"
    }
    openshift_logs.append(log)

with open('openshift_logs.json', 'w') as f:
    for log in openshift_logs:
        f.write(json.dumps(log) + '\n')

print(f"Generated {len(openshift_logs)} OpenShift logs")
print("Sample OpenShift log:")
print(json.dumps(openshift_logs[0], indent=2))
