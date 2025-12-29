
# 2. SONARQUBE LOGS
sonarqube_logs = []
sonarqube_projects = ['ecommerce-api', 'user-service', 'payment-gateway', 'notification-service']
quality_gates = ['PASSED', 'FAILED', 'WARN']

for i in range(50):
    log = {
        "timestamp": generate_timestamp(hours_ago=random.randint(0, 72)),
        "service": "sonarqube",
        "project_key": random.choice(sonarqube_projects),
        "analysis_id": f"AX{random.randint(100000, 999999)}",
        "quality_gate_status": random.choice(quality_gates),
        "bugs": random.randint(0, 50),
        "vulnerabilities": random.randint(0, 20),
        "code_smells": random.randint(0, 200),
        "coverage_percentage": round(random.uniform(50, 95), 2),
        "duplications_percentage": round(random.uniform(0, 15), 2),
        "lines_of_code": random.randint(1000, 50000),
        "log_level": random.choice(['INFO', 'WARN', 'ERROR']),
        "message": f"Code quality analysis {random.choice(['completed', 'in progress', 'failed'])}"
    }
    sonarqube_logs.append(log)

with open('sonarqube_logs.json', 'w') as f:
    for log in sonarqube_logs:
        f.write(json.dumps(log) + '\n')

print(f"Generated {len(sonarqube_logs)} SonarQube logs")
print("Sample SonarQube log:")
print(json.dumps(sonarqube_logs[0], indent=2))
