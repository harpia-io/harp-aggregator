user_filter = {
    "environments": [1],
    "destinationFilter": ["ui", "teams"],
    "notificationStatesFilter": ["active", "handled"]
}

for env in user_filter['environments']:
    for dest in user_filter['destinationFilter']:
        for state in user_filter['notificationStatesFilter']:
            print(f"{env}___{dest}___{state}")
