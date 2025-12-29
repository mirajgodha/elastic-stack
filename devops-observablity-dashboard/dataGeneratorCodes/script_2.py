
# 3. NEXUS IQ LOGS
nexus_logs = []
components = ['spring-boot:2.7.0', 'log4j:2.17.1', 'jackson-databind:2.13.0', 'commons-collections:3.2.2']
severity_levels = ['CRITICAL', 'SEVERE', 'MODERATE', 'LOW']

for i in range(50):
    log = {
        "timestamp": generate_timestamp(hours_ago=random.randint(0, 72)),
        "service": "nexus-iq",
        "application_id": random.choice(['app-prod', 'app-staging', 'app-dev']),
        "scan_id": f"scan-{random.randint(10000, 99999)}",
        "component": random.choice(components),
        "policy_violations": random.randint(0, 15),
        "critical_vulnerabilities": random.randint(0, 5),
        "severe_vulnerabilities": random.randint(0, 10),
        "moderate_vulnerabilities": random.randint(0, 20),
        "license_threats": random.randint(0, 3),
        "overall_severity": random.choice(severity_levels),
        "log_level": random.choice(['INFO', 'WARN', 'ERROR']),
        "message": f"Security scan {random.choice(['completed', 'initiated', 'failed'])}"
    }
    nexus_logs.append(log)

with open('nexus_logs.json', 'w') as f:
    for log in nexus_logs:
        f.write(json.dumps(log) + '\n')

print(f"Generated {len(nexus_logs)} Nexus IQ logs")
print("Sample Nexus IQ log:")
print(json.dumps(nexus_logs[0], indent=2))
