from flask_restful import Resource
from pylon.core.tools import log
from flask import current_app, request, make_response
from ...models.reports import UIReport
from ...models.results import UIResult
from tools import api_tools
from sqlalchemy import and_


class API(Resource):
    url_params = [
        '<int:project_id>',
    ]

    def __init__(self, module):
        self.module = module

    def __calcualte_limit(self, limit, total):
        return len(total) if limit == 'All' else limit

    def get(self, project_id: int):
        args = request.args
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        total, reports = api_tools.get(project.id, args, UIReport)
        res = []

        for report in reports:
            data = self.module.results_or_404(report.id, report)
            res.append(data)

        for each in res:
            each["start_time"] = each["start_time"].replace("T", " ").replace("Z", "")
        return {"total": total, "rows": res}

    def delete(self, project_id: int):
        args = request.args
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        query_result = UIReport.query.filter(
            and_(UIReport.project_id == project.id, UIReport.id.in_(args["id[]"]))
        ).all()

        for each in query_result:
            self.__delete_report_results(project_id, each.uid)
            each.delete()
        return {"message": "deleted"}

    def __delete_report_results(self, project_id, report_id):
        results = UIResult.query.filter_by(project_id=project_id, report_uid=report_id).all()
        for result in results:
            result.delete()
