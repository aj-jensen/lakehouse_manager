
import json
import urllib
import urllib.request

from .utils.global_config import GlobalConfig
from .pull_request_updater import PullRequestUpdater


class BranchState:

    def __init__(self, config: GlobalConfig):
        self.config = config
        self.pr_updater_instance = PullRequestUpdater(config)
        self.ALTER_VIEW = 'ALTER on VIEW'
        self.COMMIT_META = 'commitMeta'
        self.CREATE_VIEW = 'CREATE VIEW'
        self.DROP_VIEW = 'DROP VIEW'
        self.HASH = 'hash'
        self.LOG_ENTRIES = 'logEntries'
        self.MESSAGE = 'message'
        self.PARENT_COMMIT_HASH = 'parentCommitHash'

        self.last_processed_commit = ''

    def get_commit_log(self, branch):
        """
        NB: the Nessie docs (https://app.swaggerhub.com/apis/projectnessie/nessie/0.74.0#/v2/getCommitLogV2) for this
        function tell you that there is a parameter called "limit-hash" you can use to "Hash on the given ref to
        identify the commit where the operation of fetching the log should stop, i.e. the 'far' end of the commit log,
        returned late in the result." - what they mean by the 'far' end of the commit log is the part of the log that is
        closed to the HEAD of the branch!

        Put another way, this argument doesn't give you the commits between the HEAD and the arg you pass to limit-hash;
        it gives you everything between the first commit ever in the history and the limit-hash. If it gave you the
        history between HEAD and the specified commit, this function could avoid having to read in the entire commit
        history for a branch every single time it's invoked. Format of a limit-hash call for those curious:

        f"https://nessie.eu.dremio.cloud/v2/repositories/{catalog_id}/trees/main/history?limit.hash=='{commit_id}'"






        :param branch:
        :return:
        """
        url = f"https://nessie.eu.dremio.cloud/repositories/{self.config.catalog_id}/api/v2/trees/{branch}/history"

        headers = {
            'authorization': f'Bearer {self.config.dremio_cloud_secret}',
            'Content-Type': 'application/json',
            'accept': 'application/json'
        }

        # Create a request object with the URL and headers
        req = urllib.request.Request(url, headers=headers)

        try:
            # Open the URL with the request object
            with urllib.request.urlopen(req) as response:
                # Read the response data
                data = response.read().decode('utf-8')
                # Parse the JSON data
                json_data = json.loads(data)
                return json_data
        except urllib.error.HTTPError as e:
            # If there's an error, print the status code
            print(f"Error: {e.code}")
            return None

    def get_object_name(self, operation_text):
        split_on_space = operation_text.split(' ')
        return split_on_space[-1]

    def find_to_ref_commits_after_last_shared_commit(self, from_ref_commit_log, to_ref_commit_log):

        from_ref_set = set()
        first_from_ref_commit = None
        for commit in from_ref_commit_log[self.LOG_ENTRIES]:
            current_commit_id = commit[self.COMMIT_META][self.HASH]
            from_ref_set.add(current_commit_id)
            if first_from_ref_commit == None:
                first_from_ref_commit = current_commit_id

        to_ref_commits_before_last_shared_commit = []
        first_to_ref_commit = None
        for commit in to_ref_commit_log[self.LOG_ENTRIES]:

            current_commit_id = commit[self.COMMIT_META][self.HASH]
            if first_to_ref_commit == None:
                # first_to_ref_commit = current_commit_id
                print('current ', current_commit_id, ' first_from_ref_commit ', first_from_ref_commit )
                if current_commit_id == first_from_ref_commit:
                    return [[]]
                else:
                    first_to_ref_commit = current_commit_id

            if current_commit_id in from_ref_set:
                self.last_processed_commit = current_commit_id
                print('self.last_processed_commit in find_to_ref_commits_after_last_shared_commit ',
                      self.last_processed_commit)
                break
            message = commit[self.COMMIT_META][self.MESSAGE]
            to_ref_commits_before_last_shared_commit.append([current_commit_id, message])

        return to_ref_commits_before_last_shared_commit

    def check_for_view_operation(self, operation_text, view_operation):
        if len(operation_text) < len(view_operation):
            return False
        # print('in check_for_view_operation ' , operation_text[0:len(view_operation)],
        #       ' view_operation ', view_operation)
        return operation_text[0:len(view_operation)] == view_operation

    def replay_commit_log(self, commits_to_process):
        '''
        list of
        [str,str]

        first element is the commitId, e.g.:
        "d6201a45b67d215f047b0ba79cb65a526527aca04b46739dc372ccde128a1758"


        second element is message attached to the operation e.g.:
        "ALTER on VIEW application.tpcds_all_sales"

        :param commits_to_process:
        :return:
        '''

        commits_to_process = list(reversed(commits_to_process))
        for commit in commits_to_process:
            print('commit ID ', commit[0])
            operation_message = commit[1]
            if self.check_for_view_operation(operation_message, self.DROP_VIEW):
                print('Detected a drop view request ! ', operation_message)
                status_of_delete = True
            elif self.check_for_view_operation(operation_message, self.ALTER_VIEW):
                print('Detected an alter view request ! ', operation_message)
                status_of_delete = False
            elif self.check_for_view_operation(operation_message, self.CREATE_VIEW):
                print('Detected a create view request ! ', operation_message)
                status_of_delete = False
            else:
                # This means it was probably some kind of TABLE update, avoid
                continue

            view_path = self.get_object_name(operation_message)
            self.pr_updater_instance.update_sql_file_in_codecommit_branch([view_path, commit[0], operation_message,
                                                                           status_of_delete])

        return

    def kick_off(self):
        print('config.from_ref ', self.config.from_ref)
        print('config.to_ref ', self.config.to_ref)
        from_ref_commit_log = self.get_commit_log(self.config.from_ref)
        to_ref_commit_log = self.get_commit_log(self.config.to_ref)


        to_ref_commits_to_process = self.find_to_ref_commits_after_last_shared_commit(from_ref_commit_log,
                                                                                      to_ref_commit_log)

        return to_ref_commits_to_process

    def filter_commit_log(self, full_commit_log):
        print('full_commit_log in the filter_commit_log ', full_commit_log, ' self.last_processed_commit ',
              self.last_processed_commit )
        results = []
        last_proc_commit = ''
        encountered_last_proc_commit_from_prior_timestep = False
        for commit in list(reversed(full_commit_log[self.LOG_ENTRIES])):
            current_commit_id = commit[self.COMMIT_META][self.HASH]
            if encountered_last_proc_commit_from_prior_timestep:
                message = commit[self.COMMIT_META][self.MESSAGE]
                results.append([current_commit_id, message])

            if current_commit_id == self.last_processed_commit:
                encountered_last_proc_commit_from_prior_timestep = True

            last_proc_commit = current_commit_id

        self.last_processed_commit = last_proc_commit
        return results

    def processor(self):
        """
        Entrypoint into the program. If the process has just started, we invoke kick_off(). If kick_off() detects that
        there is a commit history that diverges between from_ref and to_ref, it will store the last commit it processed
        for to_ref in last_processed_commit.
            - if during a subsequent invocation processor sees that last_processed_commit value is still not set,
            it will invoke kick_off again.
            - if last_processed_commit has a value, then that means we just need to "replay" all commits on the to_ref
            Catalog branch that occurred AFTER the last_processed_commit

        The following functions:
            - find_to_ref_commits_after_last_shared_commit
            - filter_commit_log
            - replay_commit_log

        all 3 assume that the commits are returned from Nessie in reverse chronological order. that's why we only loop
        through them until we encounter a value previously seen (via from_ref in the case of filter_commit_log). This
        is also why we reverse the order of commits_to_process at the start of replay_commit_log: we need to push the
        earliest commits to CodeCommit first

        :return: None
        """
        entire_commit_log = []
        if len(self.last_processed_commit) == 0:
            entire_commit_log = self.kick_off()
            print('self.last_processed_commit at beginning of the kick_off timestep :) ', self.last_processed_commit)
            return
        else:
            entire_commit_log = self.get_commit_log(self.config.to_ref)
            print('self.last_processed_commit at beginning of non kick_off timestep ', self.last_processed_commit)

        print('entire_commit_log ', entire_commit_log)
        filtered_commit_log = self.filter_commit_log(entire_commit_log)
        print('filtered_commit_log ', filtered_commit_log)
        self.replay_commit_log(filtered_commit_log)
        print('self.last_processed_commit at beginning of non kick_off timestamp is: ', self.last_processed_commit)







