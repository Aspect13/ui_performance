from json import loads
from flask import request, make_response
from flask_restful import Resource
from ...models.reports import UIReport


class API(Resource):
    url_params = [
        '<int:project_id>/<string:report_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int, report_id: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        report = UIReport.query.filter_by(project_id=project.id, uid=report_id).first().to_json()
        return {"message": report["test_status"]["status"]}

    def put(self, project_id: int, report_id: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        report = UIReport.query.filter_by(project_id=project.id, uid=report_id).first()
        test_status = request.json["test_status"]
        report.test_status = test_status
        report.commit()
        return {"message": f"status changed to {report.test_status['status']}"}
