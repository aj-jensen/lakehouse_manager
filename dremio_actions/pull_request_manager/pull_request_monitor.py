import time

from .utils.global_config import GlobalConfig
from .utils.codecommit_boto_client import Boto3Wrapper
from .evaluator import EvaluateDataTests


class CodecommitBranchMonitor:
    def __init__(self, config: GlobalConfig, comment_poll_interval: int = 30):
        self.config = config
        self.processed_comment_id_set = set()
        self.boto_wrapper = Boto3Wrapper(config)
        self.eval_instance = EvaluateDataTests(config, self.boto_wrapper)
        self.comment_poll_interval = comment_poll_interval
        self.timer = 0
        self.ignored_users = set('GlueJobRunnerSession')
        self.SIGN_OFF = '- This commit made by the lakehouse manager, I am a bot【=◈︿◈=】'
        self.RECIPE_SIGNIFIER = '!'
        self.COMMENTS = 'comments'
        self.DELETED = 'deleted'
        self.CONTENT = 'content'
        self.AUTHOR_ARN = 'authorArn'
        self.COMMIT_ID = 'commentId'

    def generate_test_results_comment(self, data_tests_metadata: tuple[dict, int, int]) -> str:
        comment = ''
        total_count = data_tests_metadata[1]
        failed_count = data_tests_metadata[2]

        if failed_count > 0:
            comment += f"We had {failed_count} out of {total_count} data tests failed during the test run \n"

        else:
            comment += "All tests passed, LGTM \n"

        comment += self.SIGN_OFF

        return comment

    def invoke_evaluator(self) -> None:
        results_for_data_tests = self.eval_instance.run_all_tests()
        comment_for_data_test_results = self.generate_test_results_comment(results_for_data_tests)
        self.boto_wrapper.post_comment(comment_for_data_test_results)
        return

    def process_comments(self, comments_data) -> None:
        current_comments = comments_data['commentsForPullRequestData']
        # print('current_comments keys ' , current_comments.keys())
        for com in current_comments:
            print('com ', com)
            com_data = com[self.COMMENTS][0]
            print('com_data ', com_data)
            print('com_data.keys() ', com_data.keys())
            if com_data[self.DELETED]:
                continue
            contents = com_data[self.CONTENT]
            author_arn = com_data[self.AUTHOR_ARN]
            comment_id = com_data[self.COMMIT_ID]
            author = author_arn.split('/')[-1]
            print(f'comments {contents} made by author {author}')
            if (author not in self.ignored_users) and (comment_id not in self.processed_comment_id_set):
                self.processed_comment_id_set.add(comment_id)
                if contents[0] == self.RECIPE_SIGNIFIER:
                    # TODO: more parsing needed on what comes after the recipe signifier if you want more elegant apps
                    self.invoke_evaluator()
        return

    def poll_for_comments(self) -> None:
        all_comments = self.boto_wrapper.get_comments_for_pull_request()
        self.process_comments(all_comments)
        return

    def is_pull_request_closed(self) -> bool:
        return self.boto_wrapper.is_pull_request_closed()

    def is_pull_request_merged(self) -> bool:
        return self.boto_wrapper.is_pull_request_merged()


