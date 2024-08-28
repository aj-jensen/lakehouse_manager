from datetime import datetime
import boto3
from botocore import errorfactory
from dateutil.tz import tzlocal
from .global_config import GlobalConfig

class Boto3Wrapper:
    def __init__(self, config: GlobalConfig):
        """
        The entrypoint for a task that updates a CodeCommit pull request based off of the delta
        between target Arctic branch and a source Arctic branch
        """
        self.config = config
        self.client = boto3.client('codecommit')
        self.BRANCH = 'branch'
        self.BRANCH_NAME = 'branchName'
        self.COMMIT_ID = 'commitId'
        self.PULL_REQUEST_MERGE_STATE_CHANGED = 'PULL_REQUEST_MERGE_STATE_CHANGED'
        # Constants for parsing responses from the API:
        self.CLOSED = 'CLOSED'
        self.EVENT_DATE = 'eventDate'
        self.PULL_REQUEST_EVENTS = 'pullRequestEvents'
        self.PULL_REQUEST_EVENT_TYPE = 'pullRequestEventType'
        self.PULL_REQUEST_STATUS_CHANGED = 'PULL_REQUEST_STATUS_CHANGED'
        self.PULL_REQUEST_MERGE_STATE_CHANGED = 'PULL_REQUEST_MERGE_STATE_CHANGED'
        self.PULL_REQUEST_STATUS_CHANGED_EVENT_METADATA = 'pullRequestStatusChangedEventMetadata'
        self.PULL_REQUEST_MERGE_STATE_CHANGED_EVENT_METADATA = 'pullRequestMergedStateChangedEventMetadata'
        self.MERGE_METADATA = 'mergeMetadata'
        self.IS_MERGED = 'isMerged'
        self.PULL_REQUEST_STATUS = 'pullRequestStatus'

    def get_diff(self, before_commit_id, after_commit_id):
        response = self.client.get_differences(
            repositoryName=self.config.repo_name,
            beforeCommitSpecifier=before_commit_id,
            afterCommitSpecifier=after_commit_id,
        )

        return response

    def get_comments_for_pull_request_between_commits(self, before_commit, after_commit):
        response = self.client.get_comments_for_pull_request(
            pullRequestId=self.config.pull_request_id,
            repositoryName=self.config.repo_name,
            beforeCommitId=before_commit,
            afterCommitId=after_commit
        )
        return response

    def get_comments_for_pull_request(self):
        response = self.client.get_comments_for_pull_request(
            pullRequestId=self.config.pull_request_id
        )
        return response

    def get_last_commit_for_to_ref_branch(self):
        response = self.client.get_branch(
            repositoryName=self.config.repo_name,
            branchName=self.config.to_ref
        )
        print('response for  get_last_commit_for_to_ref_branch', response)
        print('**** END RESPONSE ****')

        # return response[self.BRANCH][self.BRANCH_NAME][self.COMMIT_ID]
        return response[self.BRANCH][self.COMMIT_ID]

    def get_last_commit_for_from_ref_branch(self):
        response = self.client.get_branch(
            repositoryName=self.config.repo_name,
            branchName=self.config.from_ref
        )
        print('response for get_last_commit_for_from_ref_branch', response)
        print('**** END RESPONSE ****')

        # return response[self.BRANCH][self.BRANCH_NAME][self.COMMIT_ID]
        return response[self.BRANCH][self.COMMIT_ID]

    def add_file_to_codecommit_branch(self, file_text, file_path, commit_message):
        print('file_path in add_file_to_codecommit_branch ', file_path)
        try:
            response = self.client.put_file(
                repositoryName=self.config.repo_name,
                branchName=self.config.to_ref,
                fileContent=file_text,
                filePath=file_path,
                parentCommitId=self.get_last_commit_for_to_ref_branch(),
                commitMessage=commit_message,
                name='Lakehouse Manager'
                # commitMessage='This commit made by the lakehouse manager, I am a bot 【=◈︿◈=】'
            )
        except self.client.exceptions.SameFileContentException as e:
            print(f'Just an FYI, a file with same the EXACT same text as this view {file_path} was already present in '
                  f'the branch, no big deal :) ')
            pass

    def delete_file_from_codecommit_branch(self, file_path, commit_message):

        try:
            response = self.client.delete_file(
                repositoryName=self.config.repo_name,
                branchName=self.config.to_ref,
                filePath=file_path,
                parentCommitId=self.get_last_commit_for_to_ref_branch(),
                keepEmptyFolders=False,
                # commitMessage='This commit made by the lakehouse manager, I am a bot 【=◈︿◈=】',
                commitMessage=commit_message,
                name='Lakehouse Manager'
            )
        except self.client.exceptions.FileDoesNotExistException as e:
            print('INFO: We tried to remove a file from a codecommit branch that didn\'t actually have that file,')

    def get_file(self, file_path, reference):
        response = self.client.get_file(
            repositoryName=self.config.repo_name,
            commitSpecifier=reference,
            filePath=file_path
        )
        return response

    def post_comment(self, comment):
        response = self.client.post_comment_for_pull_request(
                    pullRequestId=self.config.pull_request_id,
                    repositoryName=self.config.repo_name,
                    # Remember that from_ref is the name of the Arctic branch you're trying to merge into,
                    # typically `main`, as such it should be the branch you're trying to merge into in codecommit
                    beforeCommitId=self.get_last_commit_for_from_ref_branch(),
                    afterCommitId=self.get_last_commit_for_to_ref_branch(),
                    content=comment,
                    # clientRequestToken='string'
                    )
        print(response)

    def get_all_pull_request_events(self):
        response = self.client.describe_pull_request_events(
            pullRequestId=self.config.pull_request_id
        )

        return response

    def check_pull_request_status_matches(self, status_to_check):
        # Maximum number of results that can be returned is here
        # There is a paginator version of this function available here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/codecommit/paginator/DescribePullRequestEvents.html
        response = self.client.describe_pull_request_events(
            pullRequestId=self.config.pull_request_id,
            pullRequestEventType=status_to_check,
        )
        return response

    def is_pull_request_merged(self) -> bool:
        response = self.check_pull_request_status_matches(self.PULL_REQUEST_MERGE_STATE_CHANGED)
        for event in response[self.PULL_REQUEST_EVENTS]:
            if event[self.PULL_REQUEST_EVENT_TYPE] == self.PULL_REQUEST_MERGE_STATE_CHANGED:
                if event[self.PULL_REQUEST_MERGE_STATE_CHANGED_EVENT_METADATA][self.MERGE_METADATA][self.IS_MERGED]:
                    return True

        return False

    def is_pull_request_closed(self) -> bool :
        response = self.get_all_pull_request_events()
        for event in response[self.PULL_REQUEST_EVENTS]:
            if event[self.PULL_REQUEST_EVENT_TYPE] == self.PULL_REQUEST_STATUS_CHANGED:
                if event[self.PULL_REQUEST_STATUS_CHANGED_EVENT_METADATA][self.PULL_REQUEST_STATUS] == self.CLOSED:
                    return True

        return False

