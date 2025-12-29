
# 4. TWISTLOCK (Prisma Cloud) LOGS
twistlock_logs = []
image_names = ['nginx:latest', 'redis:6.2', 'postgres:14', 'node:16-alpine']
compliance_results = ['PASS', 'FAIL', 'WARN']

for i in range(50):
    log = {
        "timestamp": generate_timestamp(hours_ago=random.randint(0, 72)),
        "service": "twistlock",
        "scan_type": random.choice(['image', 'container', 'host', 'serverless']),
        "image_name": random.choice(image_names),
        "registry": random.choice(['docker.io', 'gcr.io', 'ecr']),
        "critical_cves": random.randint(0, 5),
        "high_cves": random.randint(0, 15),
        "medium_cves": random.randint(0, 30),
        "low_cves": random.randint(0, 50),
        "compliance_status": random.choice(compliance_results),
        "runtime_threats": random.randint(0, 3),
        "namespace": random.choice(['production', 'staging', 'development']),
        "log_level": random.choice(['INFO', 'WARN', 'ERROR', 'CRITICAL']),
        "message": f"Container security scan {random.choice(['completed', 'detected threats', 'clean'])}"
    }
    twistlock_logs.append(log)

with open('twistlock_logs.json', 'w') as f:
    for log in twistlock_logs:
        f.write(json.dumps(log) + '\n')

print(f"Generated {len(twistlock_logs)} Twistlock logs")
print("Sample Twistlock log:")
print(json.dumps(twistlock_logs[0], indent=2))
