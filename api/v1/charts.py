import itertools

from flask_restful import Resource
from pylon.core.tools import log
from flask import request
from ...models.results import UIResult
from ...models.reports import UIReport


class API(Resource):
    url_params = [
        '<int:project_id>/<string:report_id>',
        '<int:project_id>/<int:report_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id, report_id):
        args = request.args
        chart = {
                "nodes": [

                ],
                "edges": [
                ]
            }
        if isinstance(report_id, int):
            report = UIReport.query.get_or_404(report_id)
            results = UIResult.query.filter_by(project_id=project_id, report_uid=report.uid).order_by(
                UIResult.session_id,
                UIResult.id).all()
        else:
            report = UIReport.query.filter_by(project_id=project_id, uid=report_id).first_or_404()
            results = UIResult.query.filter_by(project_id=project_id, report_uid=report_id).order_by(
                UIResult.session_id,
                UIResult.id).all()

        nodes, edges = self.build_graph(project_id, results, report.aggregation, report.loops, args["metric"])

        chart["nodes"] = nodes
        chart["edges"] = edges

        return chart

    def build_graph(self, project_id, results, aggregation, loops, metric="total"):
        edges = []
        nodes = self.get_nodes(results)
        flow = self.get_flow(results)
        threshold_results = self.assert_threshold(results, aggregation, metric)

        for session, steps in flow.items():
            for curr, upcoming in self.pairwise(steps):
                current_node = self.find_node(curr, nodes)
                upcoming_node = self.find_node(upcoming, nodes)
                if self.is_edge_exists(current_node, upcoming_node, edges):
                    continue

                edge = self.make_edge(current_node, upcoming_node)
                edges.append(edge)

        for node in nodes:
            result = self.find_result(node, results)
            if not result:
                continue

            threshold_result = threshold_results[result.identifier]
            status = threshold_result['status']
            result = threshold_result['data']
            time = round(threshold_result['time'] / 1000, 2)
            node['status'] = status
            node['file'] = f"/api/v1/artifacts/artifact/{project_id}/reports/{result.file_name}"

            edge = self.find_edge(result, edges)
            if len(edge) == 1:
                edge[0]['data']['time'] = time
                edge[0]["classes"] = status
            if len(edge) > 1:
                edge[0]['data']['time'] = time
                edge[0]["classes"] = status

        return nodes, edges

    def get_nodes(self, steps):
        nodes = [
            {"data": {"id": 'start', "name": 'Start', "identifier": "start_point", "session_id": "start"}}
        ]
        for result in steps:
            current_node = self.find_if_exists(result, nodes)
            if not current_node:
                current_node = self.result_to_node(result)
                nodes.append(current_node)
        return nodes

    def find_if_exists(self, curr_node, node_list):
        res = list(filter(lambda x: x['data']['identifier'] == curr_node.identifier, node_list))
        if len(res) == 1:
            return res[0]
        if len(res) > 1:
            raise Exception("Bug! Node duplication")
        return None

    def result_to_node(self, res):
        return {
            "data": {
                "id": res.id,
                "name": res.name,
                "session_id": res.session_id,
                "identifier": res.identifier,
                "type": res.type,
                "status": "passed",
                "result_id": res.id,
                "file": f"/api/v1/artifacts/artifact/{res.project_id}/reports/{res.file_name}"
            }
        }

    def get_flow(self, steps):
        flows = {}
        start = {"data": {"id": 'start', "name": 'Start', "identifier": "start_point", "session_id": "start"}}
        for step in steps:
            curr_session_id = step.session_id
            if curr_session_id in flows.keys():
                flows[curr_session_id].append(self.result_to_node(step))
            else:
                flows[curr_session_id] = [start, self.result_to_node(step)]
        return flows

    def pairwise(self, iterable):
        "s -> (s0,s1), (s1,s2), (s2, s3), ..."
        a, b = itertools.tee(iterable)
        next(b, None)
        return zip(a, b)

    def find_node(self, curr_node, node_list):
        if curr_node['data']['identifier'] == 'start_point':
            return curr_node

        res = list(filter(lambda x: x['data']['identifier'] == curr_node['data']['identifier'], node_list))
        if len(res) == 1:
            return res[0]
        if len(res) > 1:
            raise Exception("Bug! Node duplication")
        return None

    def find_edge(self, res, edges):
        res = list(filter(lambda x: x['data']['id_to'] == res.identifier, edges))
        return res

    def is_edge_exists(self, node1, node2, edges):
        res = list(
            filter(lambda e: e['data']['source'] == node1['data']['id'] and e['data']['target'] == node2['data']['id'],
                   edges))
        return len(res) > 0

    def make_edge(self, node_from, node_to):
        return {
            "data": {
                "source": node_from['data']['id'],
                "target": node_to['data']['id'],
                "session_id": node_from['data']['session_id'],
                "id_to": node_to['data']['identifier'],
                "time": ""
            },
            "classes": ""
        }

    def find_result(self, node, results):
        res = list(filter(lambda r: r.id == node['data']['id'], results))
        if len(res) == 1:
            return res[0]
        return None

    def assert_threshold(self, results, aggregation, metric="total"):
        graph_aggregation = {}
        for result in results:
            if result.identifier in graph_aggregation.keys():
                graph_aggregation[result.identifier].append(result)
            else:
                graph_aggregation[result.identifier] = [result]

        threshold_results = {}
        for name, values in graph_aggregation.items():
            aggregated_total = self.get_aggregated_data(aggregation, values, metric)
            result = self.closest(values, aggregated_total)
            thresholds_failed = sum([d.thresholds_failed for d in values])
            status = "passed"
            if thresholds_failed > 0:
                status = "failed"
            threshold_results[name] = {"status": status, "data": result, "time": aggregated_total}
        return threshold_results

    def get_aggregated_data(self, aggregation, values, metric="total"):
        totals = [d.to_json()[metric] for d in values]
        if aggregation == "max":
            return max(totals)
        elif aggregation == "min":
            return min(totals)
        elif aggregation == "avg":
            return sum(totals) / len(totals)

    def closest(self, lst, val):
        return lst[min(range(len(lst)), key=lambda i: abs(lst[i].total - val))]

