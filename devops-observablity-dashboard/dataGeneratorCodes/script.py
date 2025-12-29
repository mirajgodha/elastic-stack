
import json
import random
from datetime import datetime, timedelta

# Generate sample logs for each service
def generate_timestamp(days_ago=0, hours_ago=0):
    """Generate timestamp in ISO format"""
    base_time = datetime.now() - timedelta(days=days_ago, hours=hours_ago)
    return base_time.isoformat()

# 1. JENKINS LOGS
jenkins_logs = []
jenkins_statuses = ['SUCCESS', 'FAILURE', 'UNSTABLE', 'ABORTED']
jenkins_jobs = ['microservice-build', 'deployment-pipeline', 'integration-tests', 'security-scan']

for i in range(50):
    log = {
        "timestamp": generate_timestamp(hours_ago=random.randint(0, 72)),
        "service": "jenkins",
        "job_name": random.choice(jenkins_jobs),
        "build_number": random.randint(100, 500),
        "status": random.choice(jenkins_statuses),
        "duration_ms": random.randint(30000, 600000),
        "executor": f"executor-{random.randint(1, 5)}",
        "user": random.choice(['admin', 'developer1', 'devops-user']),
        "log_level": random.choice(['INFO', 'WARN', 'ERROR']),
        "message": f"Build {random.choice(['started', 'completed', 'failed', 'triggered'])}"
    }
    jenkins_logs.append(log)

# Save Jenkins logs
with open('jenkins_logs.json', 'w') as f:
    for log in jenkins_logs:
        f.write(json.dumps(log) + '\n')

print(f"Generated {len(jenkins_logs)} Jenkins logs")
print("Sample Jenkins log:")
print(json.dumps(jenkins_logs[0], indent=2))
