
# 6. JIRA LOGS
jira_logs = []
issue_types = ['Bug', 'Story', 'Task', 'Epic', 'Sub-task']
priorities = ['Highest', 'High', 'Medium', 'Low', 'Lowest']
statuses = ['To Do', 'In Progress', 'Code Review', 'Done', 'Blocked']

for i in range(50):
    log = {
        "timestamp": generate_timestamp(hours_ago=random.randint(0, 72)),
        "service": "jira",
        "issue_key": f"PROJ-{random.randint(1000, 9999)}",
        "issue_type": random.choice(issue_types),
        "priority": random.choice(priorities),
        "status": random.choice(statuses),
        "assignee": random.choice(['john.doe', 'jane.smith', 'bob.wilson', None]),
        "reporter": random.choice(['john.doe', 'jane.smith', 'admin']),
        "project_key": random.choice(['DEVOPS', 'INFRA', 'SECURITY', 'APP']),
        "action": random.choice(['created', 'updated', 'transitioned', 'commented', 'assigned']),
        "time_spent_hours": round(random.uniform(0, 8), 2) if random.random() > 0.5 else None,
        "story_points": random.choice([1, 2, 3, 5, 8, 13, None]),
        "log_level": random.choice(['INFO', 'WARN', 'ERROR']),
        "message": f"Issue {random.choice(['created', 'updated', 'resolved', 'closed'])}"
    }
    jira_logs.append(log)

with open('jira_logs.json', 'w') as f:
    for log in jira_logs:
        f.write(json.dumps(log) + '\n')

print(f"Generated {len(jira_logs)} JIRA logs")
print("Sample JIRA log:")
print(json.dumps(jira_logs[0], indent=2))
