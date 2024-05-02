from .utils.cloud_query_runner import CloudQuery
from .utils.codecommit_boto_client import Boto3Wrapper
from .utils.global_config import GlobalConfig
from .sql_keywords import DremioLexiconPhrases


class PullRequestUpdater:
    def __init__(self, config: GlobalConfig, poll_limit=30, pause_time=1):
        """
        The entrypoint for a task that updates a CodeCommit pull request based off of the delta
        between target Arctic branch and a source Arctic branch
        """
        self.config = config
        self.boto_wrapper = Boto3Wrapper(config)
        self.dc_poll_limit = poll_limit
        self.dc_pause_time = pause_time
        self.RESULTS_ROWS_KEY = 'rows'
        self.RESULTS_ROWS_INDEX = 0
        self.RESULTS_SQL_DEF_KEY = 'sql_definition'
        self.SQL_FILE_EXTENSION = '.sql'
        self.SLASH = '/'

    def assemble_show_create_view_at_branch_query(self, view_name) -> str:
        query_text = DremioLexiconPhrases.SHOW_CREATE_VIEW + f'{view_name}' + \
                     DremioLexiconPhrases.AT_BRANCH + f'{self.config.to_ref}'
        return query_text

    @staticmethod
    def assemble_show_create_view_at_commit_query(view_name, commit_id) -> str:
        query_text = DremioLexiconPhrases.SHOW_CREATE_VIEW + f'{view_name}' + \
                     DremioLexiconPhrases.AT_COMMIT + f'"{commit_id}"'
        return query_text

    def parse_results_from_show_create_view_query(self, view_definition_query_results: dict) -> str:
        return view_definition_query_results[self.RESULTS_ROWS_KEY][self.RESULTS_ROWS_INDEX][self.RESULTS_SQL_DEF_KEY]

    def get_view_text_at_branch(self, view_name: str) -> str:
        print('view_name in get_view_text_at_branch ', view_name)
        full_view_name = self.config.repo_name + view_name
        print('full_view_name  ', full_view_name)
        view_text_at_branch_cloud_query = CloudQuery(self.config.project_id, self.config.dremio_cloud_region,
                                                     self.config.dremio_cloud_secret, self.dc_poll_limit,
                                                     self.dc_pause_time)

        view_definition_at_branch_query_text = self.assemble_show_create_view_at_branch_query(full_view_name)
        view_definition_at_branch_results = view_text_at_branch_cloud_query.submit_query_and_poll_for_response(
                                                                view_definition_at_branch_query_text
                                             )

        view_text = self.parse_results_from_show_create_view_query(view_definition_at_branch_results)
        return view_text

    def get_view_text_at_commit(self, view_name: str, commit_id: str) -> str:
        print('view_name in get_view_text_at_branch ', view_name)
        full_view_name = self.config.repo_name + view_name
        print('full_view_name  ', full_view_name)
        view_text_at_branch_cloud_query = CloudQuery(self.config.project_id, self.config.dremio_cloud_region,
                                                     self.config.dremio_cloud_secret, self.dc_poll_limit,
                                                     self.dc_pause_time)

        view_definition_at_commit_query_text = PullRequestUpdater.assemble_show_create_view_at_commit_query(
                                                                        full_view_name, commit_id)
        view_definition_at_commit_results = view_text_at_branch_cloud_query.submit_query_and_poll_for_response(
                                                                view_definition_at_commit_query_text
                                             )

        view_text = self.parse_results_from_show_create_view_query(view_definition_at_commit_results)

        return view_text

    def add_sql_file_to_branch(self, view_text, view_path, message='') -> dict:
        response = self.boto_wrapper.add_file_to_codecommit_branch(view_text, view_path, message)
        return response

    def delete_sql_file_from_branch(self, view_path, message='') -> dict:
        response = self.boto_wrapper.delete_file_from_codecommit_branch(view_path, message)
        return response

    def generate_sql_file_path_and_name(self, view_name) -> str:
        sql_file_path_and_name = (self.config.path_from_codecommit_root_to_catalog_mirror +
                                  self.SLASH + view_name)
        sql_file_path_and_name = sql_file_path_and_name.replace('.', '/') + self.SQL_FILE_EXTENSION
        return sql_file_path_and_name

    def get_arctic_qualified_name(self, view_name) -> str:
        return self.config.path_from_codecommit_root_to_catalog_mirror + '.' + view_name

    def update_sql_file_in_codecommit_branch(self, view_delta: list[str, bool, str]) -> None:
        print(f'update_sql_file_in_codecommit_branch has this value for view_delta: {view_delta}')

        view_name = view_delta[0]
        commit_id = view_delta[1]
        message = view_delta[2]
        is_deleted = view_delta[3]

        view_sql_file_path_and_name = self.generate_sql_file_path_and_name(view_name)
        print(f'view_sql_file_path_and_name in update_sql_file_in_codecommit_branch has is deleted_value {is_deleted}',
              view_sql_file_path_and_name)

        if is_deleted:
            self.delete_sql_file_from_branch(view_sql_file_path_and_name, message)
        else:
            # view_definition_text = self.get_view_text_at_branch(self.get_arctic_qualified_name(view_name))
            # self.add_sql_file_to_branch(view_definition_text, view_sql_file_path_and_name)
            view_definition_text = self.get_view_text_at_commit(self.get_arctic_qualified_name(view_name), commit_id)
            self.add_sql_file_to_branch(view_definition_text, view_sql_file_path_and_name, message)

        return
