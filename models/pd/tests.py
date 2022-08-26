from pylon.core.tools import log
from typing import Optional
from uuid import uuid4
from pydantic import BaseModel, validator, AnyUrl, parse_obj_as, root_validator, constr
import string

from tools import rpc_tools

from ...constants import RUNNER_MAPPING


class TestOverrideable(BaseModel):
    parallel_runners: Optional[int]
    location: str = 'default'
    env_vars: dict = {}
    customization: dict = {}
    cc_env_vars: dict = {}

    @root_validator(pre=True, allow_reuse=True)
    def empty_str_to_none(cls, values):
        removed = []
        for k in list(values.keys()):
            if values[k] == '':
                removed.append(k)
                del values[k]
        return values

    @validator('customization')
    def validate_customization(cls, value: dict):
        for k, v in list(value.items()):
            if any((k, v)):
                assert all((k, v)), 'All fields must be filled'
            else:
                del value[k]
        return value

    @validator('env_vars')
    def validate_env_vars(cls, value: dict):
        value['ENV'] = value.get('ENV', 'Default')
        return value


class TestCommon(TestOverrideable):
    """
    Model of test itself without test_params or other plugin module's data
    """
    project_id: int
    test_uid: Optional[str]
    name: str
    test_type: str
    env_type: str
    parallel_runners: int
    entrypoint: str
    runner: str
    source: dict
    browser: str = 'Chrome_undefined'
    loops: Optional[int]
    aggregation: Optional[str]

    @root_validator
    def set_uuid(cls, values: dict):
        if not values.get('test_uid'):
            values['test_uid'] = str(uuid4())
        return values

    @validator('name', 'test_type', 'env_type')
    def check_allowed_chars(cls, value: str):
        try:
            int(value[0])
            assert False, 'Can not start with a number'
        except ValueError:
            ...

        valid_chars = f'{string.ascii_letters}{string.digits}_'
        assert all(c in valid_chars for c in value), 'Only letters, numbers and "_" allowed'
        return value

    @validator('runner')
    def validate_runner(cls, value: str):
        assert value in RUNNER_MAPPING.keys(), \
            "Runner not supported. Available runners: {}".format(
                list(RUNNER_MAPPING.keys())
            )
        return value

    @validator('source')
    def validate_sources(cls, value: dict):
        validated = rpc_tools.RpcMixin().rpc.call.parse_source(value)
        return {
            'name': value['name'],
            **validated.dict()
        }

    @validator('aggregation')
    def validate_aggregation(cls, value: str):
        valid_aggregations = ['max', 'min', 'avg']
        assert value in valid_aggregations, f'Only following aggregations are supported: {valid_aggregations}'
        return value
