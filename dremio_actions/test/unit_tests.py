import json

from dremio_actions.pull_request_manager.catalog_branch_updater import CatalogBranchUpdater
from dremio_actions.pull_request_manager.pull_request_updater import PullRequestUpdater
from dremio_actions.pull_request_manager.evaluator import EvaluateDataTests
from dremio_actions.pull_request_manager.utils.stub_global_config import StubGlobalConfig
from dremio_actions.pull_request_manager.utils.codecommit_boto_client import Boto3Wrapper

from dremio_actions.pull_request_manager.branch_state import BranchState

stub_gc = StubGlobalConfig('', '', '', 'DremioData', 'main', 'dev', '', 'DremioData', '', '')


def test_get_view_definition_at_branch():
    pr_updater_test = PullRequestUpdater(stub_gc)
    test = pr_updater_test.get_view_text_at_branch('preparation.test_view_6')
    print('test results ', test)
    return test

def test_state_branch_check_view():
    bs = BranchState(stub_gc)
    t = 'DROP VIEW preparation.test_view_6'

    print('substring , ', t[0:len(bs.DROP_VIEW)])
    print(bs.check_for_view_operation(t, bs.DROP_VIEW),)

def test_find_to_ref_commits_after_last_shared_commit_with_diff_last_commit():
    """
    Branch 2 is the branch that is "ahead" in this scenario (i.e. the dev branch that has more commits than main)
    Note that the operations in these commits were randomly generated and might not make sense, they weren't needed
    to for the purpose of testing. The only guarantee is that parentCommitHash matches the commitMeta['hash'] value
    for the next element in the array (i.e. the array can correctly be modeled as a linked list if you so choose)
    :return:
    """
    bs = BranchState(stub_gc)
    branch_1_commit_log_file = None
    with open('./full_commit_log_nessie_branch_1.json','r') as branch_1_data:
        branch_1_commit_log_file = json.load(branch_1_data)

    branch_2_commit_log_file = None
    with open('./full_commit_log_nessie_branch_2.json','r') as branch_2_data:
        branch_2_commit_log_file = json.load(branch_2_data)

    test_result = bs.find_to_ref_commits_after_last_shared_commit(branch_1_commit_log_file, branch_2_commit_log_file)
    return test_result[0][0] == '39dc372ccde128aa04b46d215f047b0ba7971758d6201a45b67cb65a526527ac'

def test_find_to_ref_commits_after_last_shared_commit_with_same_last_commit():
    """

    :return:
    """
    bs = BranchState(stub_gc)
    branch_1_commit_log_file = None
    with open('./full_commit_log_nessie_branch_1.json','r') as branch_1_data:
        branch_1_commit_log_file = json.load(branch_1_data)

    branch_2_commit_log_file = None
    with open('./full_commit_log_nessie_branch_3.json','r') as branch_2_data:
        branch_2_commit_log_file = json.load(branch_2_data)

    test_result = bs.find_to_ref_commits_after_last_shared_commit(branch_1_commit_log_file, branch_2_commit_log_file)
    return len(test_result[0]) == 0

def test_merge_statement():
    catalog_updater = CatalogBranchUpdater(stub_gc)
    print(catalog_updater.assemble_merge_statement_query())

def test_evaluator():
    boto_wrapper = Boto3Wrapper(stub_gc)
    ev = EvaluateDataTests(stub_gc, boto_wrapper)
    test_query = 'select * from \"some_table\" at branch {branch}'
    query_gen_test = ev.generate_query_with_branch_name(test_query, stub_gc.to_ref)
    print('query_gen_test ', query_gen_test)
    return query_gen_test == 'select * from \"some_table\" at branch dev'


if __name__ == '__main__':
    '''
    
    '''
    test_find_to_ref_commits_after_last_shared_commit_with_diff_last_commit = (
        test_find_to_ref_commits_after_last_shared_commit_with_diff_last_commit())
    commits_after_last_shared_commit_with_diff_last_commit = (
        test_find_to_ref_commits_after_last_shared_commit_with_same_last_commit())
    test_evaluator()
