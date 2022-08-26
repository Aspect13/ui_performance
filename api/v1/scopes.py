from flask import request
from flask_restful import Resource
from pylon.core.tools import log

from ...models.reports import UIReport
from ...models.results import UIResult


class API(Resource):
    url_params = [
        '<int:project_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        scopes = UIResult.query.with_entities(UIResult.name).filter(
            UIResult.report_uid.in_(
                UIReport.query.with_entities(UIReport.uid).filter(
                    UIReport.project_id == project.id,
                    UIReport.name == request.args['name'],
                    UIReport.environment == request.args['environment']
                )
            ),
        ).distinct(UIResult.name).all()
        return [i[0] for i in scopes], 200
