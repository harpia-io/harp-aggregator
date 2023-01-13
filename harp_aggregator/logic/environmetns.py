import requests
import harp_aggregator.settings as settings
from microservice_template_core.tools.logger import get_logger
from harp_aggregator.metrics.service_monitoring import Prom

logger = get_logger()
studios = requests.get(f'http://{settings.ENVIRONMENTS_HOST}/api/v1/environments/all').json()
config = {'studios_dict': studios, 'studios_id_lst': requests.get(f'http://{settings.ENVIRONMENTS_HOST}/api/v1/environments/all').json().keys(), 'counter': 0}


@Prom.UPDATE_ENVIRONMENT_DICT.time()
def update_environment_dict(old_config):
    try:
        studios_dict = requests.get(f'http://{settings.ENVIRONMENTS_HOST}/api/v1/environments/all').json()
        if studios_dict:
            old_config['studios_dict'] = studios_dict
            # logger.info(f"Environments dictionary from environment-service updated with: {studios_dict}",
            #             extra={"event_name": "Update studios dictionary"})
        studios_id_lst = requests.get(f'http://{settings.ENVIRONMENTS_HOST}/api/v1/environments/all').json().keys()
        if studios_id_lst:
            old_config['studios_id_lst'] = studios_id_lst

    except Exception as exc:
        logger.error(f"Can't collect environments dictionary from environment-service",
                        extra={"event_name": "Collect studios dictionary", "exception": str(exc)})
        if not studios:
            raise Exception("Can't collect environments dictionary from environment-service")
