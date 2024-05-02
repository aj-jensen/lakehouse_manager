from .utils.cloud_query_runner import CloudQuery
from .utils.global_config import GlobalConfig
from .sql_keywords import DremioLexiconPhrases


class CatalogBranchUpdater:
    def __init__(self, config: GlobalConfig):
        self.config = config
        self.cloud_query_runner = CloudQuery(self.config.project_id, self.config.dremio_cloud_region,
                                             self.config.dremio_cloud_secret)

    def assemble_merge_statement_query(self) -> str:
        query = (DremioLexiconPhrases.MERGE_BRANCH + self.config.to_ref + DremioLexiconPhrases.INTO
                 + self.config.from_ref + DremioLexiconPhrases.IN + self.config.repo_name)
        return query

    def merge_catalog_branch(self) -> None:
        merge_query_text = self.assemble_merge_statement_query()
        print('merge_query_text ', merge_query_text)
        self.cloud_query_runner.submit_query_and_poll_for_response(merge_query_text)
