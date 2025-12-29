
# 5. CONFLUENCE LOGS
confluence_logs = []
actions = ['page_created', 'page_updated', 'page_deleted', 'comment_added', 'attachment_uploaded', 'space_created']
spaces = ['Engineering', 'Product', 'DevOps', 'HR', 'Finance']

for i in range(50):
    log = {
        "timestamp": generate_timestamp(hours_ago=random.randint(0, 72)),
        "service": "confluence",
        "action": random.choice(actions),
        "user": random.choice(['john.doe', 'jane.smith', 'admin', 'bob.wilson']),
        "space_key": random.choice(spaces),
        "page_id": f"page-{random.randint(1000, 9999)}",
        "session_id": f"sess-{random.randint(100000, 999999)}",
        "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
        "response_time_ms": random.randint(50, 2000),
        "status_code": random.choice([200, 201, 400, 403, 500]),
        "log_level": random.choice(['INFO', 'WARN', 'ERROR']),
        "message": f"User performed {random.choice(actions)}"
    }
    confluence_logs.append(log)

with open('confluence_logs.json', 'w') as f:
    for log in confluence_logs:
        f.write(json.dumps(log) + '\n')

print(f"Generated {len(confluence_logs)} Confluence logs")
print("Sample Confluence log:")
print(json.dumps(confluence_logs[0], indent=2))
