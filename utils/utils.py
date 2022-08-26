import json
from datetime import datetime
from queue import Empty
from typing import List, Tuple, Union, Optional
from uuid import uuid4
from pydantic import ValidationError

from pylon.core.tools import log

from tools import task_tools, rpc_tools


def test_parameter_value_by_name(test_parameters: List[dict], param_name: str) -> Optional[str]:
    for i in test_parameters:
        if i['name'] == param_name:
            return i['default']


def run_test(test: 'UIPerformanceTest', config_only: bool = False, execution: bool = False) -> dict:
    event = test.configure_execution_json(execution=execution)

    if config_only:
        return event

    from ..models.reports import UIReport
    report = UIReport(
        uid=uuid4(),
        name=test_parameter_value_by_name(test.all_test_parameters.dict()['test_parameters'], 'test_name'),
        project_id=test.project_id,
        start_time=datetime.utcnow().isoformat(" ").split(".")[0],
        is_active=True,
        browser=test.browser_name,
        browser_version=test.browser_version,  # todo: same value as browser?
        environment=test_parameter_value_by_name(test.test_parameters, 'env_type'),
        test_type=test_parameter_value_by_name(test.test_parameters, 'test_type'),
        loops=test.loops,
        aggregation=test.aggregation,
        test_config=test.api_json(),
        test_uid=test.test_uid
    )
    report.insert()
    event["cc_env_vars"]["REPORT_ID"] = str(report.uid)

    resp = task_tools.run_task(test.project_id, [event])
    # resp['redirect'] = f'/task/{resp["task_id"]}/results'  # todo: where this should lead to?

    test.rpc.call.increment_statistics(test.project_id, 'ui_performance_test_runs')
    resp['result_id'] = report.id  # for test rerun
    return resp


class ValidationErrorPD(Exception):
    def __init__(self, loc: Union[str, list], msg: str):
        self.loc = [loc] if isinstance(loc, str) else loc
        self.msg = msg
        super().__init__({'loc': self.loc, 'msg': msg})

    def json(self):
        return json.dumps(self.dict())

    def dict(self):
        return {'loc': self.loc, 'msg': self.msg}


def parse_test_data(project_id: int, request_data: dict,
                    *,
                    rpc=None, common_kwargs: dict = None,
                    test_create_rpc_kwargs: dict = None,
                    raise_immediately: bool = False,
                    skip_validation_if_undefined: bool = True,
                    ) -> Tuple[dict, list]:
    """
    Parses data while creating test

    :param project_id: Project id
    :param request_data: data from request json to validate
    :param rpc: instance of rpc_manager or None(will be initialized)
    :param common_kwargs: kwargs for common_test_parameters
            (test parameters apart from test_params table. E.g. name, description)
    :param test_create_rpc_kwargs: for each test_data key a rpc is called - these kwargs will be passed to rpc call
    :param raise_immediately: weather to raise validation error on first encounter or raise after collecting all errors
    :param skip_validation_if_undefined: if no rpc to validate test_data key is found
            data will remain untouched if True or erased if False
    :return:
    """
    if not rpc:
        rpc = rpc_tools.RpcMixin().rpc

    common_kwargs = common_kwargs or dict()
    test_create_rpc_kwargs = test_create_rpc_kwargs or dict()
    errors = list()

    common_params = request_data.pop('common_params', {})

    try:
        test_data = rpc.call.ui_performance_test_create_common_parameters(
            project_id=project_id,
            test_params=common_params,
            **common_kwargs
        )
    except ValidationError as e:
        test_data = dict()
        errors.extend(e.errors())
        if raise_immediately:
            return test_data, errors

    for k, v in request_data.items():
        try:
            test_data.update(rpc.call_function_with_timeout(
                func=f'ui_performance_test_create_{k}',
                timeout=2,
                data=v,
                **test_create_rpc_kwargs
            ))
        except Empty:
            log.warning(f'Cannot find parser for {k}')
            if skip_validation_if_undefined:
                test_data.update({k: v})
        except ValidationError as e:
            for i in e.errors():
                i['loc'] = [k, *i['loc']]
            errors.extend(e.errors())

            if raise_immediately:
                return test_data, errors
        except Exception as e:
            from traceback import format_exc
            log.warning(f'Exception as e {type(e)} in ui_performance_test_create_{k}\n{format_exc()}')
            e.loc = [k, *getattr(e, 'loc', [])]
            errors.append(ValidationErrorPD(e.loc, str(e)).dict())
            if raise_immediately:
                return test_data, errors

    return test_data, errors
