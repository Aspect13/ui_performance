from pydantic import BaseModel, validator, AnyUrl, parse_obj_as, root_validator, constr

from ..ui_report import UIReport
from ...models.ui_tests import UIPerformanceTest


class ThresholdPD(BaseModel):
    project_id: int
    test: str
    environment: str
    scope: str
    target: str
    aggregation: str
    comparison: str
    value: float

    @validator('test')
    def validate_test_exists(cls, value: str, values: dict):
        assert UIPerformanceTest.query.filter(
            UIPerformanceTest.project_id == values['project_id'],
            UIPerformanceTest.name == value
        ).first(), f'Test with name {value} does not exist'
        return value

    @validator('environment')
    def validate_env_exists(cls, value: str, values: dict):
        assert UIReport.query.filter(
            UIReport.environment == value,
            UIReport.project_id == values['project_id']
        ).first(), 'Result with this environment does not exist'
        return value

    @validator('scope')
    def validate_scope_exists(cls, value: str, values: dict):
        if value in ['all', 'every']:
            return value

        assert UIReport.query.filter(
            UIReport.project_id == values['project_id'],
            UIReport.requests.contains(value)
        ).first(), 'Such scope does not exist'
        return value

    @validator('target')
    def validate_target(cls, value: str):
        assert value in {
            'total', 'time_to_first_byte', 'time_to_first_paint',
            'dom_content_loading', 'dom_processing', 'speed_index',
            'time_to_interactive', 'first_contentful_paint', 'largest_contentful_paint',
            'cumulative_layout_shift', 'total_blocking_time', 'first_visual_change',
            'last_visual_change'
        }, f'Target {value} is not supported'
        return value

    @validator('aggregation')
    def validate_aggregation(cls, value: str):
        assert value in {'max', 'min', 'avg', 'pct95', 'pct50'}, f'Aggregation {value} is not supported'
        return value

    @validator('comparison')
    def validate_comparison(cls, value: str):
        assert value in {'gte', 'lte', 'lt', 'gt', 'eq'}, f'Comparison {value} is not supported'
        return value
