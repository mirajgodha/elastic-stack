
# 8. ANSIBLE LOGS
ansible_logs = []
playbooks = ['deploy-app.yml', 'configure-servers.yml', 'security-hardening.yml', 'backup-databases.yml']
tasks = ['Installing packages', 'Configuring firewall', 'Copying files', 'Restarting services', 'Running health checks']

for i in range(50):
    log = {
        "timestamp": generate_timestamp(hours_ago=random.randint(0, 72)),
        "service": "ansible",
        "playbook": random.choice(playbooks),
        "play": random.choice(['Configure web servers', 'Deploy application', 'Update security', 'Backup data']),
        "task": random.choice(tasks),
        "host": f"server-{random.randint(1, 20)}.example.com",
        "status": random.choice(['ok', 'changed', 'failed', 'skipped', 'unreachable']),
        "execution_time_seconds": round(random.uniform(0.5, 30), 2),
        "changed": random.choice([True, False]),
        "failed": random.choice([True, False]) if random.random() > 0.8 else False,
        "inventory_group": random.choice(['webservers', 'databases', 'loadbalancers', 'monitoring']),
        "ansible_user": random.choice(['ansible-bot', 'devops-admin']),
        "log_level": random.choice(['INFO', 'WARN', 'ERROR']),
        "message": f"Task {random.choice(['completed successfully', 'failed', 'skipped', 'resulted in changes'])}"
    }
    ansible_logs.append(log)

with open('ansible_logs.json', 'w') as f:
    for log in ansible_logs:
        f.write(json.dumps(log) + '\n')

print(f"Generated {len(ansible_logs)} Ansible logs")
print("Sample Ansible log:")
print(json.dumps(ansible_logs[0], indent=2))
