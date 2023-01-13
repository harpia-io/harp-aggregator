from microservice_template_core import Core
from microservice_template_core.settings import ServiceConfig, FlaskConfig, DbConfig
from apscheduler.schedulers.background import BackgroundScheduler
from harp_aggregator.logic.aggregator import update_aerospike
from harp_aggregator.logic.environmetns import update_environment_dict, config
import harp_aggregator.settings as settings
from harp_aggregator.endpoints.health import ns as health


def scheduler_jobs():
    import logging
    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.ERROR)
    scheduler = BackgroundScheduler({'apscheduler.job_defaults.max_instances': '1'})
    scheduler.add_job(update_environment_dict, 'interval', args=[config], seconds=settings.UPDATE_ENVIRONMENTS_SECONDS)
    scheduler.add_job(update_aerospike, 'interval', seconds=settings.UPDATE_AEROSPIKE_SECONDS)
    scheduler.start()


def main():
    ServiceConfig.configuration['namespaces'] = [health]
    FlaskConfig.FLASK_DEBUG = False
    DbConfig.USE_DB = False
    scheduler_jobs()
    app = Core()
    app.run()


if __name__ == '__main__':
    main()

