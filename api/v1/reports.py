from traceback import format_exc

from flask_restful import Resource
from pylon.core.tools import log
from flask import current_app, request, make_response

from ...models.pd.test_parameters import UITestParamsRun
from ...models.reports import UIReport
from tools import MinioClient, api_tools
from datetime import datetime

from ...models.tests import UIPerformanceTest


class API(Resource):
    url_params = [
        '<int:project_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):
        args = request.args
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        if args.get("report_id"):
            report = UIReport.query.filter_by(project_id=project.id, id=args.get("report_id")).first().to_json()
            return report
        reports = []
        total, res = api_tools.get(project.id, args, UIReport)
        for each in res:
            each_json = each.to_json()
            each_json["start_time"] = each_json["start_time"].replace("T", " ").split(".")[0]
            reports.append(each_json)
        return {"total": total, "rows": reports}

    def post(self, project_id: int):
        args = request.json

        report = UIReport.query.filter_by(uid=args['report_id']).first()

        if report:
            return report.to_json()

        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)

        test_config = None
        if 'test_params' in args:
            try:
                test = UIPerformanceTest.query.filter(
                    UIPerformanceTest.test_uid == args.get('test_id')  # todo: no test_uid
                ).first()
                # test._session.expunge(test) # maybe we'll need to detach object from orm
                test.__dict__['test_parameters'] = test.filtered_test_parameters_unsecret(
                    UITestParamsRun.from_control_tower_cmd(
                        args['test_params']
                    ).dict()['test_parameters']
                )
            except Exception as e:
                log.error('Error parsing params from control tower %s', format_exc())
                return f'Error parsing params from control tower: {e}', 400

        report = UIReport(
            uid=args['report_id'],
            name=args["test_name"],
            project_id=project.id,
            start_time=args["time"],
            is_active=True,
            browser=args["browser_name"],
            browser_version=args["browser_version"],
            environment=args["env"],
            loops=args["loops"],
            aggregation=args["aggregation"]
        )
        if test_config:
            report.test_config = test_config
        report.insert()

        self.module.context.rpc_manager.call.increment_statistics(project_id, 'ui_performance_test_runs')
        return report.to_json()

    def put(self, project_id: int):
        args = request.json
        report = UIReport.query.filter_by(project_id=project_id, uid=args['report_id']).first_or_404()
        report.is_active = False
        report.stop_time = args["time"]
        report.test_status = args["status"]
        report.thresholds_total = args["thresholds_total"],
        report.thresholds_failed = args["thresholds_failed"]
        report.duration = self.__diffdates(report.start_time, args["time"]).seconds
        exception = args["exception"]
        if exception:
            report.exception = exception
            report.passed = False

        report.commit()

        return report.to_json()

    def delete(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            delete_ids = list(map(int, request.args["id[]"].split(',')))
        except TypeError:
            return make_response('IDs must be integers', 400)
        UIReport.query.filter(
            UIReport.project_id == project.id,
            UIReport.id.in_(delete_ids)
        ).delete()
        UIReport.commit()
        return {"message": "deleted"}, 204

    def __diffdates(self, d1, d2):
        # Date format: %Y-%m-%d %H:%M:%S
        date_format = '%Y-%m-%d %H:%M:%S'
        return datetime.strptime(d2, date_format) - datetime.strptime(d1, date_format)
