from microservice_template_core.tools.flask_restplus import api
from flask_restx import Resource
from microservice_template_core.tools.logger import get_logger


logger = get_logger()

ns = api.namespace('health', description='Health endpoint')


@ns.route('')
class Health(Resource):

    def get(self):
        """
            Return general health check result
        """

        return {"msg": "Healthy"}, 200
