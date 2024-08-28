import time

from pull_request_manager.branch_state import BranchState
from pull_request_manager.catalog_branch_updater import CatalogBranchUpdater
from pull_request_manager.pull_request_monitor import CodecommitBranchMonitor
from pull_request_manager.utils.global_config import GlobalConfig
from pull_request_manager.utils.cloud_secrets import CloudSecrets


def run(project_id, catalog_id, dremio_cloud_region, repo_name, from_ref, to_ref, pull_request_id,
        path_from_codecommit_root_to_catalog_mirror, dremio_cloud_pat_aws_secrets_manager_secret_name,
        path_to_tests, aws_region):


    print('path_to_tests in runner.run(), ', path_to_tests )
    cloud_secret = CloudSecrets(aws_region, dremio_cloud_pat_aws_secrets_manager_secret_name)
    secret = cloud_secret.get_secret('PAT')

    global_configuration = GlobalConfig(project_id, catalog_id, dremio_cloud_region, repo_name, from_ref, to_ref,
                                        pull_request_id, path_from_codecommit_root_to_catalog_mirror, secret,
                                        path_to_tests)

    branch_state_logger = BranchState(global_configuration)
    pull_request_monitor = CodecommitBranchMonitor(global_configuration)
    catalog_branch_updater = CatalogBranchUpdater(global_configuration)
    while True:
        branch_state_logger.processor()
        time.sleep(20)
        pull_request_monitor.poll_for_comments()
        if pull_request_monitor.is_pull_request_closed():
            print('detected a closed PR, ending the job')
            break
        if pull_request_monitor.is_pull_request_merged():
            print('detected a merged PR, running the merge and ending the job')
            catalog_branch_updater.merge_catalog_branch()
            print('merge successful = ')
            break
