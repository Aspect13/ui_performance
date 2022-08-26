from sqlalchemy import and_
from flask import request
from flask_restful import Resource

from pydantic import ValidationError

from ...models.thresholds import UIThreshold
from ...models.pd.thresholds import ThresholdPD

from tools import api_tools


class API(Resource):
    url_params = [
        '<int:project_id>',
        '<int:project_id>/<int:threshold_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        if request.args.get("test") and request.args.get("env"):
            res = UIThreshold.query.filter(and_(
                UIThreshold.project_id == project.id,
                UIThreshold.test == request.args.get("test"),
                UIThreshold.environment == request.args.get("env")
            )).all()
            return [th.to_json() for th in res], 200
        total, res = api_tools.get(project_id, request.args, UIThreshold)
        return {'total': total, 'rows': [i.to_json() for i in res]}, 200

    def post(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            pd_obj = ThresholdPD(project_id=project_id, **request.json)
        except ValidationError as e:
            return e.errors(), 400
        th = UIThreshold(**pd_obj.dict())
        th.insert()
        return th.to_json(), 201

    def delete(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            delete_ids = list(map(int, request.args["id[]"].split(',')))
        except TypeError:
            return 'IDs must be integers', 400

        UIThreshold.query.filter(
            UIThreshold.project_id == project.id,
            UIThreshold.id.in_(delete_ids)
        ).delete()
        UIThreshold.commit()
        return {'ids': delete_ids}, 204

    def put(self, project_id: int, threshold_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            pd_obj = ThresholdPD(project_id=project_id, **request.json)
        except ValidationError as e:
            return e.errors(), 400
        th_query = UIThreshold.query.filter(
            UIThreshold.project_id == project_id,
            UIThreshold.id == threshold_id
        )
        th_query.update(pd_obj.dict())
        UIThreshold.commit()
        return th_query.one().to_json(), 200
