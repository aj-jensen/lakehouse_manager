import json

from .utils.global_config import GlobalConfig
from .utils.cloud_query_runner import CloudQuery
from .utils.codecommit_boto_client import Boto3Wrapper

class EvaluateDataTests:
    def __init__(self, config: GlobalConfig, boto_wrapper_instance: Boto3Wrapper):
        self.config = config
        self.boto_wrapper_instance = boto_wrapper_instance
        # Some queries might take a while since they may be analytical in nature -
        # poll_limit should be a little high
        self.cloud_query = CloudQuery(self.config.project_id, self.config.dremio_cloud_region,
                                    self.config.dremio_cloud_secret)
        self.tests = 'tests'
        self.type = 'type'
        self.query = 'query'
        self.expected = 'expected'
        self.RESULTS_ROWS_KEYS = 'rows'
        self.RESULTS_ROWS_INDEX = 0
        self.SORTED_AGGREGATION_TEST = 'sorted_aggregation'
        self.SCALAR_TEST = 'scalar'
        self.COLUMN_NAME = 'column_name'
        self.NAME = 'name'
        self.FILE_CONTENT = 'fileContent'

    def get_data_tests(self):
        """We do the to_ref (dev branch) version of the data_tests file, because maybe there is a good reason for one of
        the tests to change :P
        """
        # latest_commit = self.boto_wrapper_instance.get_last_commit_for_to_ref_branch()
        print('path to tests from config ', self.config.path_to_tests)
        tests_file_get_request = self.boto_wrapper_instance.get_file(self.config.path_to_tests, self.config.to_ref)
        return json.loads(tests_file_get_request[self.FILE_CONTENT].decode())

    def post_test_results_to_pull_request(self, comment):
        self.boto_wrapper_instance.post_comment(comment)
        return

    # def parse_results_from_show_create_view_query(self, view_definition_query_results: dict) -> str:
    #     return view_definition_query_results[self.results_rows_key][self.results_rows_index][self.results_sql_def_key]

    def parse_query_test_result(self, test, result):
        if test[self.type] == self.SORTED_AGGREGATION_TEST:
            results_list = []
            for i in range(len(result[self.RESULTS_ROWS_KEYS])):
                row_result = result[self.RESULTS_ROWS_KEYS][i][test[self.COLUMN_NAME]]
                results_list.append(row_result)
            return results_list
        elif test[self.type] == self.SCALAR_TEST:
            return result[self.RESULTS_ROWS_KEYS][self.RESULTS_ROWS_INDEX][test[self.COLUMN_NAME]]

        return

    def generate_query_with_branch_name(self, query, branch_name) -> str:
        print(f'generate_query_with_branch_name branch_name for the evaluator code is {branch_name}')
        query = query.replace('{branch}', branch_name)
        return query

    def run_test(self, current_test) -> bool:
        query = self.generate_query_with_branch_name(current_test['query'], self.config.to_ref)
        result = self.cloud_query.submit_query_and_poll_for_response(query)
        print('result in run_test ', result)
        if self.parse_query_test_result(current_test, result) != current_test['expected']:
            return False
        return True

    def run_all_tests(self) -> tuple[dict, int, int]:
        tests_dict = self.get_data_tests()
        print('tests_dict in run_all_tests: ', tests_dict)
        results = {}
        counter = 0
        fail_counter = 0
        for test in tests_dict[self.tests]:
            # print()
            counter += 1
            test_result = self.run_test(test)
            if not test_result:
                results[test[self.NAME]] = test_result
                fail_counter += 1
        return results, counter, fail_counter

