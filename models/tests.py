#     Copyright 2021 getcarrier.io
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from queue import Empty
from typing import List, Union, Optional
from pylon.core.tools import log
from sqlalchemy import Column, Integer, String, JSON, ARRAY, and_

from tools import db_tools, db, rpc_tools, constants as c, secrets_tools

from .pd.execution_json import ExecutionParams, CcEnvVars
from .pd.test_parameters import UITestParams
from ..constants import RUNNER_MAPPING


class UIPerformanceTest(db_tools.AbstractBaseMixin, db.Base, rpc_tools.RpcMixin):
    __tablename__ = 'ui_performance_tests'
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, unique=False, nullable=False)
    test_uid = Column(String(128), unique=True, nullable=False)
    name = Column(String(128), nullable=False)
    browser = Column(String(128), nullable=False)
    loops = Column(Integer, nullable=True)
    aggregation = Column(String(20), nullable=True)
    parallel_runners = Column(Integer, nullable=False)
    location = Column(String(128), nullable=False)
    entrypoint = Column(String(128), nullable=False)
    runner = Column(String(128), nullable=False)
    test_parameters = Column(ARRAY(JSON), nullable=True)
    integrations = Column(JSON, nullable=True)
    schedules = Column(ARRAY(Integer), nullable=True, default=[])
    env_vars = Column(JSON)
    customization = Column(JSON)
    cc_env_vars = Column(JSON)
    source = Column(JSON, nullable=False)

    @property
    def container(self):
        return RUNNER_MAPPING.get(self.runner)

    @property
    def job_type(self):
        return 'observer'

    @property
    def default_params_mapping(self) -> dict:
        return {"test_name": self.name}

    @property
    def default_test_parameters(self) -> UITestParams:
        return UITestParams(test_parameters=[
            {'name': k, 'default': v, 'description': 'default parameter'}
            for k, v in self.default_params_mapping.items()
        ])

    @property
    def all_test_parameters(self) -> UITestParams:
        tp = self.default_test_parameters
        tp.update(UITestParams.from_orm(self))
        return tp

    @property
    def docker_command(self):
        cmd_template = 'docker run -t --rm ' \
                       '-e project_id={project_id} ' \
                       '-e galloper_url={galloper_url} ' \
                       '-e token={token} ' \
                       'getcarrier/control_tower:{control_tower_version} ' \
                       '--test_id={test_id}'
        return cmd_template.format(
            project_id=self.project_id,
            galloper_url=secrets_tools.unsecret("{{secret.galloper_url}}", project_id=self.project_id),
            token=secrets_tools.unsecret("{{secret.auth_token}}", project_id=self.project_id),
            control_tower_version=c.CURRENT_RELEASE,
            test_id=self.test_uid
        )

    @property
    def browser_name(self) -> str:
        return self.browser.split('_')[0]

    @property
    def browser_version(self) -> Optional[Union[int, str]]:
        try:
            return self.browser.split('_')[1]
        except IndexError:
            return None

    def add_schedule(self, schedule_data: dict, commit_immediately: bool = True) -> None:
        schedule_data['test_id'] = self.id
        schedule_data['project_id'] = self.project_id
        try:
            schedule_id = self.rpc.timeout(2).scheduling_ui_performance_create_schedule(data=schedule_data)
            updated_schedules = set(self.schedules)
            updated_schedules.add(schedule_id)
            self.schedules = list(updated_schedules)
            if commit_immediately:
                self.commit()
        except Empty:
            log.warning('No scheduling rpc found')

    def handle_change_schedules(self, schedules_data: List[dict]) -> None:
        new_schedules_ids = set(i['id'] for i in schedules_data if i['id'])
        ids_to_delete = set(self.schedules).difference(new_schedules_ids)
        self.schedules = []
        for s in schedules_data:
            self.add_schedule(s, commit_immediately=False)
        try:
            self.rpc.timeout(2).scheduling_delete_schedules(ids_to_delete)
        except Empty:
            ...
        self.commit()

    @classmethod
    def get_api_filter(cls, project_id: int, test_id: Union[int, str]):
        if isinstance(test_id, int):
            return and_(
                cls.project_id == project_id,
                cls.id == test_id
            )
        return and_(
            cls.project_id == project_id,
            cls.test_uid == test_id
        )

    def configure_execution_json(self, execution=False):
        execution_json = {
            "test_id": self.test_uid,
            "container": self.container,
            "execution_params": ExecutionParams.from_orm(self).dict(exclude_none=True),
            "cc_env_vars": CcEnvVars.from_orm(self).dict(exclude_none=True),
            "job_name": self.name,
            "job_type": self.job_type,
            "concurrency": self.parallel_runners,
            "channel": self.location,
            **self.rpc.call.parse_source(self.source).execution_json,
            "integrations": self.integrations
        }
        if execution:
            execution_json = secrets_tools.unsecret(execution_json, project_id=self.project_id)
        return execution_json

    def to_json(self, exclude_fields: tuple = (), keep_custom_test_parameters: bool = True, with_schedules: bool = False) -> dict:
        test = super().to_json(exclude_fields=exclude_fields)
        if 'job_type' not in exclude_fields:
            test['job_type'] = self.job_type
        if test.get('test_parameters') and 'test_parameters' not in exclude_fields:
            if keep_custom_test_parameters:
                exclude_fields = set(exclude_fields) - set(
                    i.name for i in UITestParams.from_orm(self).test_parameters
                )
            test['test_parameters'] = self.all_test_parameters.exclude_params(
                exclude_fields
            ).dict()['test_parameters']
        if with_schedules and 'schedules' not in exclude_fields:
            schedules = test.pop('schedules', [])
            if schedules:
                try:
                    test['schedules'] = self.rpc.timeout(
                        2).scheduling_ui_performance_load_from_db_by_ids(schedules)
                except Empty:
                    test['schedules'] = []
        return test

    def api_json(self, with_schedules: bool = False):
        return self.to_json(
            exclude_fields=tuple(
                tp.name for tp in self.default_test_parameters.test_parameters
                if tp.name != 'test_name'  # leave test_name here
            ),
            keep_custom_test_parameters=True,  # explicitly
            with_schedules=with_schedules
        )
