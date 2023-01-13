import json

import pymysql
from microservice_template_core.tools.logger import get_logger
import traceback
from collections import defaultdict
from harp_aggregator.logic.environmetns import config
from microservice_template_core.tools.aerospike_client import AerospikeClient
import harp_aggregator.settings as settings
import time
from harp_aggregator.metrics.service_monitoring import Prom

aerospike_client_environments = AerospikeClient(
    aerospike_set=f'{settings.SERVICE_NAMESPACE}_environments',
    bin_index={'guid': 'string'}
)

aerospike_client_aggr_notifications = AerospikeClient(
    aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_notifications',
    bin_index={'guid': 'string'}
)

aerospike_client_aggr_statistics = AerospikeClient(
    aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_statistics',
    bin_index={'guid': 'string'}
)

logger = get_logger()
harp_new_db = {
    'host': settings.DBHOST,
    'user': settings.DBUSER,
    'password': settings.DBPASS,
    'charset': 'utf8mb4',
    'database': settings.DBSCHEMA
}

consolidation_queries = {
    "Consolidated by: Alert Name": {
        "time": 5, "columns_to_show": ["alert_name"]
    },
    "Consolidated by: Object Name": {
        "time": 5, "columns_to_show": ["host"]
    }
}

where_ = {
    "active": """ downtime_expire_ts < NOW() AND snooze_expire_ts < NOW() 
                  AND acknowledged = '0' AND assign_status = '0' AND handle_expire_ts < NOW()""",
    "snoozed": """ snooze_expire_ts >= NOW() """,
    "acknowledged": """ acknowledged != '0' """,
    "in_downtime": """ downtime_expire_ts >= NOW() """,
    "assigned": """ assign_status = '1' """,
    "handled": """ handle_expire_ts >= NOW() """,
    "all": """ notification_status = '1' """  # Show only active alerts (However in active_alerts table we can see only active)
}


def flatten_dict(pyobj, keystring=''):
    if type(pyobj) == dict:
        keystring = keystring + '___' if keystring else keystring
        for k in pyobj:
            yield from flatten_dict(pyobj[k], keystring + str(k))
    else:
        yield keystring, pyobj


def prepare_env_stats(notifications):
    env_stats = {}
    for env in notifications:
        env_stats[env] = {}
        for destination in notifications[env]:
            env_stats[env][destination] = {}
            for state in notifications[env][destination]:
                env_stats[env][destination][state] = notifications[env][destination][state]['statistics']

    return env_stats


def update_aerospike():
    notification_list = prepare_notifications()
    environments_stats = prepare_env_stats(notification_list)
    flatten_notifications = {k: v for k, v in flatten_dict(notification_list)}

    aerospike_client_environments.put_message(
        aerospike_set=f'{settings.SERVICE_NAMESPACE}_environments',
        aerospike_key='statistics',
        aerospike_message=environments_stats
    )

    for key, value in flatten_notifications.items():
        if '_notifications' in key:
            message = {'notifications': value}
            aerospike_client_aggr_notifications.put_message(
                aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_notifications',
                aerospike_key=key,
                aerospike_message=message
            )
        elif '_statistics' in key:
            message = {'statistics': value}
            aerospike_client_aggr_statistics.put_message(
                aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_statistics',
                aerospike_key=key,
                aerospike_message=message
            )
        else:
            logger.error(msg=f"Unknown key - {key}, value - {value}")


@Prom.UPDATE_AEROSPIKE_CACHE.time()
def prepare_notifications():
    db = pymysql.connect(**harp_new_db)
    cursor_db = db.cursor()
    db.commit()

    notifications = {}
    for environment in config['studios_id_lst']:
        notifications[environment] = {}
        for destination_id, destination_name in settings.NOTIFICATION_DESTINATION.items():
            notifications[environment][destination_name] = {}
            for notification_state in settings.NOTIFICATION_STATES:
                notifications[environment][destination_name][notification_state] = {}

                alerts = collect_active_alerts(
                    environment=int(environment),
                    notification_destination=destination_id,
                    notification_state=notification_state,
                    cursor_db=cursor_db
                )

                notifications[environment][destination_name][notification_state]['notifications'] = alerts['alerts']
                notifications[environment][destination_name][notification_state]['statistics'] = alerts['count']

    db.close()
    return notifications


def convert_all_alerts(all_alerts_raw):
    alerts_stats = {"alerts": {}, "alerts_list": [], "consolidation_lists": {}}
    alerts_count = len(all_alerts_raw)

    for row in all_alerts_raw:
        alert_id, alert_name, studio, ms, source, service, object_name, severity, total_duration, notification_type, \
        notification_status, department, additional_fields, ms_alert_id, acknowledged, consolidation_name, consolidation_state, \
        consolidation_id, consolidation_ts, created_ts, downtime_expire_ts, snooze_expire_ts, handle_expire_ts, assigned_to, action_by = row
        try:
            duration = time.time() - created_ts
            alert_body = {
                "alert_id": alert_id,
                "alert_name": alert_name,
                "monitoring_system": ms,
                "severity": severity,
                "source": source,
                "service": service,
                "host": object_name,
                "studio": config['studios_dict'][str(studio)],
                "notification_status": notification_status,
                "notification_type": notification_type,
                "consolidation_name": consolidation_name,
                "consolidation_state": consolidation_state,
                "consolidation_id": consolidation_id,
                "consolidation_ts": consolidation_ts,
                "created_ts": created_ts,
                "ms_alert_id": ms_alert_id,
                "duration": duration,
                "total_duration": total_duration,
                "acknowledged": False if acknowledged == 0 else True,
                "downtime_expire_ts": downtime_expire_ts,
                "snooze_expire_ts": snooze_expire_ts,
                "handle_expire_ts": handle_expire_ts,
                "department": [],
                "additional_fields": json.loads(additional_fields),
                "assigned_to": json.loads(assigned_to),
                "action_by": json.loads(action_by)
            }

            alerts_stats["alerts"][alert_id] = alert_body
            if consolidation_id != 0:
                if consolidation_id not in alerts_stats["alerts_list"]:
                    alerts_stats["alerts_list"].append(consolidation_id)
                    consolidation_fields = defaultdict(dict)
                    for column_to_show in consolidation_queries[consolidation_name]['columns_to_show']:
                        consolidation_fields[column_to_show] = alert_body[column_to_show]
                    alerts_stats["consolidation_lists"][consolidation_id] = {
                        "consolidation_name": consolidation_name,
                        "consolidation_fields": consolidation_fields,
                        "consolidation_time": consolidation_queries[consolidation_name]['time'],
                        "duration": time.time() - consolidation_ts + consolidation_queries[consolidation_name]['time']*60,
                        "alerts": [],
                    }
                alerts_stats["consolidation_lists"][consolidation_id]["alerts"].append(alert_id)
            else:
                alerts_stats["alerts_list"].append(alert_id)
        except Exception as exc:
            logger.error(msg=f"Convert all alerts. Row: {row}. Exception: {exc}.",
                            extra={"event_name": "Convert all alerts", "raw_value": traceback.format_exc()})
    return alerts_stats, alerts_count


@Prom.REFORMAT_ALERTS.time()
def reformat_alerts(alerts, notification_state):
    notifications = []
    for alert_id in alerts['alerts_list']:
        if alert_id < 100000000:
            alert_body = {
                "alert_body": {
                    "notification_id": alert_id,
                    "panel_type": "single_alert",
                    "last_change_ts": int(alerts['alerts'][alert_id]['created_ts']),
                    "current_duration": int(alerts['alerts'][alert_id]['duration']),
                    "notification_state": notification_state,
                    "notification_status": alerts['alerts'][alert_id]['notification_status'],
                    "assigned_to": alerts['alerts'][alert_id]['assigned_to'],
                    "action_by": alerts['alerts'][alert_id]['action_by'],
                    "body": {
                        "monitoring_system": alerts['alerts'][alert_id]['monitoring_system'],
                        "service": alerts['alerts'][alert_id]['service'],
                        "source": alerts['alerts'][alert_id]['source'],
                        "name": alerts['alerts'][alert_id]['alert_name'],
                        "severity": alerts['alerts'][alert_id]['severity'],
                        "department": alerts['alerts'][alert_id]['department'],
                        "additional_fields": alerts['alerts'][alert_id]['additional_fields'],
                        "notification_type": alerts['alerts'][alert_id]['notification_type'],
                        "studio": alerts['alerts'][alert_id]['studio'],
                        "object": alerts['alerts'][alert_id]['host'],
                        "current_duration": int(alerts['alerts'][alert_id]['duration']),
                        "total_duration": int(alerts['alerts'][alert_id]['total_duration']) + int(
                            alerts['alerts'][alert_id]['duration'])
                    }
                }
            }

            alert_tags = {
                "monitoring_system": alerts['alerts'][alert_id]['monitoring_system'],
                "source": alerts['alerts'][alert_id]['source'],
                "name": alerts['alerts'][alert_id]['alert_name'],
                "severity": alerts['alerts'][alert_id]['severity'],
                **alerts['alerts'][alert_id]['additional_fields']
            }

            full_alert = {**alert_body, **alert_tags}

            notifications.append(full_alert)
        else:
            consolidated_alerts = []
            max_severity = 0
            if len(alerts['consolidation_lists'][alert_id]['alerts']) > 1:
                for cons_alert_id in alerts['consolidation_lists'][alert_id]['alerts']:
                    max_severity = max(max_severity, alerts['alerts'][cons_alert_id]['severity'])
                    alert_body = {
                        "notification_id": cons_alert_id,
                        "panel_type": "single_alert",
                        "last_change_ts": int(alerts['alerts'][cons_alert_id]['created_ts']),
                        "current_duration": int(alerts['alerts'][cons_alert_id]['duration']),
                        "notification_state": notification_state,
                        "notification_status": alerts['alerts'][cons_alert_id]['notification_status'],
                        "assigned_to": alerts['alerts'][cons_alert_id]['assigned_to'],
                        "action_by": alerts['alerts'][cons_alert_id]['action_by'],
                        "body": {
                            "monitoring_system": alerts['alerts'][cons_alert_id]['monitoring_system'],
                            "service": alerts['alerts'][cons_alert_id]['service'],
                            "source": alerts['alerts'][cons_alert_id]['source'],
                            "name": alerts['alerts'][cons_alert_id]['alert_name'],
                            "severity": alerts['alerts'][cons_alert_id]['severity'],
                            "department": alerts['alerts'][cons_alert_id]['department'],
                            "additional_fields": alerts['alerts'][cons_alert_id]['additional_fields'],
                            "notification_type": alerts['alerts'][cons_alert_id]['notification_type'],
                            "studio": alerts['alerts'][cons_alert_id]['studio'],
                            "object": alerts['alerts'][cons_alert_id]['host'],
                            "current_duration": int(alerts['alerts'][cons_alert_id]['duration']),
                            "total_duration": int(alerts['alerts'][cons_alert_id]['total_duration']) + int(alerts['alerts'][cons_alert_id]['duration'])
                        }
                    }

                    alert_tags = {
                        "monitoring_system": alerts['alerts'][cons_alert_id]['monitoring_system'],
                        "source": alerts['alerts'][cons_alert_id]['source'],
                        "name": alerts['alerts'][cons_alert_id]['alert_name'],
                        "severity": alerts['alerts'][cons_alert_id]['severity'],
                    }

                    alert_tags = {**alert_tags, **alerts['alerts'][cons_alert_id]['additional_fields']}

                    full_alert = {
                        "alert_body": alert_body,
                        "alert_tags": alert_tags
                    }

                    consolidated_alerts.append(full_alert)
            else:
                alert_id_cons_single = alerts['consolidation_lists'][alert_id]['alerts'][0]
                alert_body = {
                    "notification_id": alert_id_cons_single,
                    "panel_type": "single_alert",
                    "last_change_ts": int(alerts['alerts'][alert_id_cons_single]['created_ts']),
                    "current_duration": int(alerts['alerts'][alert_id_cons_single]['duration']),
                    "notification_state": notification_state,
                    "new_alert": True,
                    "notification_status": alerts['alerts'][alert_id_cons_single]['notification_status'],
                    "flapping": 100,
                    "assigned_to": alerts['alerts'][alert_id_cons_single]['assigned_to'],
                    "action_by": alerts['alerts'][alert_id_cons_single]['action_by'],
                    "body": {
                        "monitoring_system": alerts['alerts'][alert_id_cons_single]['monitoring_system'],
                        "service": alerts['alerts'][alert_id_cons_single]['service'],
                        "source": alerts['alerts'][alert_id_cons_single]['source'],
                        "name": alerts['alerts'][alert_id_cons_single]['alert_name'],
                        "severity": alerts['alerts'][alert_id_cons_single]['severity'],
                        "department": alerts['alerts'][alert_id_cons_single]['department'],
                        "additional_fields": alerts['alerts'][alert_id_cons_single]['additional_fields'],
                        "notification_type": alerts['alerts'][alert_id_cons_single]['notification_type'],
                        "studio": alerts['alerts'][alert_id_cons_single]['studio'],
                        "object": alerts['alerts'][alert_id_cons_single]['host'],
                        "current_duration": int(alerts['alerts'][alert_id_cons_single]['duration']),
                        "total_duration": int(alerts['alerts'][alert_id_cons_single]['duration'])
                    }
                }

                alert_tags = {
                    "monitoring_system": alerts['alerts'][alert_id_cons_single]['monitoring_system'],
                    "source": alerts['alerts'][alert_id_cons_single]['source'],
                    "name": alerts['alerts'][alert_id_cons_single]['alert_name'],
                    "severity": alerts['alerts'][alert_id_cons_single]['severity'],
                }

                alert_tags = {**alert_tags, **alerts['alerts'][alert_id_cons_single]['additional_fields']}

                full_alert = {
                    "alert_body": alert_body,
                    "alert_tags": alert_tags
                }

                notifications.append(full_alert)
            notifications.append({
                "notification_id": alert_id,
                "panel_type": "consolidation",
                "current_duration": alerts['consolidation_lists'][alert_id]['duration'],
                "body": {
                    "consolidation_name": alerts['consolidation_lists'][alert_id]["consolidation_name"],
                    "consolidation_fields": alerts['consolidation_lists'][alert_id]['consolidation_fields'],
                    "current_duration": alerts['consolidation_lists'][alert_id]['duration'],
                    "notifications": consolidated_alerts,
                    "severity": max_severity,
                },
            })

    print(json.dumps(notifications))

    return notifications


@Prom.COLLECT_ACTIVE_ALERTS.time()
def collect_active_alerts(cursor_db, environment: int, notification_destination: int, notification_state: str):
    studio_clause = f"AND studio = '{environment}'"

    where = where_[notification_state] + studio_clause + f" AND notification_type = '{notification_destination}'"

    query = f""" SELECT alert_id, alert_name, studio, ms, source, service, object_name, severity, total_duration, 
    notification_type, notification_status, department, additional_fields, ms_alert_id, acknowledged, consolidation_name, 
    consolidation_state, consolidation_id, UNIX_TIMESTAMP(consolidation_ts), UNIX_TIMESTAMP(created_ts), 
    UNIX_TIMESTAMP(downtime_expire_ts), UNIX_TIMESTAMP(snooze_expire_ts), UNIX_TIMESTAMP(handle_expire_ts), assigned_to, action_by FROM  active_alerts 
    WHERE {where} ORDER BY created_ts DESC; """
    cursor_db.execute(query)
    active_alerts = cursor_db.fetchall()
    if len(active_alerts) > 0:
        pass
    else:
        active_alerts = []
    raw_alerts, alerts_count = convert_all_alerts(active_alerts)
    checked_alerts = reformat_alerts(raw_alerts, notification_state)

    return {'alerts': checked_alerts, 'count': alerts_count}
