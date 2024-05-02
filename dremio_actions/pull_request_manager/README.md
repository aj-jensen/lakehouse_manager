

**Lakehouse Manager** is a system that monitors a Dremio Cloud instance's catalog - specifically, it tracks the
state of a Catalog branch in Dremio, and keeps that branch in sync with a "mirror image" of the branch in a version
control system like AWS CodeCommit



To start off, for the very first time:
1) Create a repo in CodeCommit, and name the repo after your catalog in DremioCloud
2) Create a dev branch off of main in Dremio Cloud, make any changes to your view definitions (or add new ones, delete
existing, etc.)
3) Make a dev branch off of main in CodeCommit as well, give it the same name as the branch in the Dremio Catalog. You
*do not* need to make any changes to the CodeCommit dev branch on your local or in the CodeCommit console
4) Open a pull request for merging the dev branch into main. CodeCommit will say something about there not being any
changes that's fine, you can ignore it
5) Your changes in the Dremio Catalog will get pulled into the CodeCommit branch automatically. When you're happy with
the branch, you can merge the PR in CodeCommit and the corresponding Catalog branch will be merged into main in Dremio


However, in order for all of the above to work, you have to have a running instance of the Lakehouse Manager :)
The entrypoint into the lakehouse manager is **runner.py**, specifically the function **run** which takes the below arguments:

**project_id** --> ID of the Dremio Cloud project, can be found in the General Information tab of your Project Settings page

**catalog_id** --> ID of the Nessie Catalog, in the General info tab of your Catalog Settings page 

**dremio_cloud_region** --> Used for calls to Dremio's public facing APIs. 'eu' or 'emea' if your Cloud deployment is in the
EMEA region, otherwise any other string will default to US Dremio Cloud API endpoint.  

**repo_name** --> Name of your repo in CodeCommit

**from_ref** --> Name of the branch you want to merge into (typically, main)

**to_ref** --> Name of the branch you're merging into main ('dev', 'test', 'aj-bugfix-12', etc.)

**pull_request_id** --> the id of the pull request from CodeCommit

**path_from_codecommit_root_to_catalog_mirror** --> You can pass an empty string. It's used sparingly, mainly just in place 
right now in case of some future enhancements (e.g. what if we have a repo with things other than the Catalog mirror,
and the Catalog mirror is just a folder that lives next to a bunch of pipeline or ML packages written in Python, etc.)

**dremio_cloud_pat_aws_secrets_manager_secret_name** --> the name of an AWS Secrets Manager Secret that holds your PAT
for Dremio Cloud. The code in runner.py expects the key within the secret to just be 'PAT' - if you provide it that then
it will call utils.cloud_secrets.py on your behalf and get the secret value for you. For best results, the Secret should
be in the same region as the CodeCommit repo

**path_to_tests** --> path to get to a special file in the CodeCommit repo that DOES NOT mirror the Catalog. This file
holds the "data-test" test cases

**aws_region** --> region in AWS, e.g. 'us-west-2'

Quick overview of the more important parts of the code:
1. branch_state --> After a PR is opened, and the Lakehouse Manager is kicked off, the BranchState class
polls continuously for any changes between the dev branch and main, *specifically* looking for any altere/created/deleted
views in the Catalog. in the event of a delta being detected, the delta is handed off to pull_request_updater.py, which 
updates the dev branch in CodeCommit with the CRUD operation 
2. pull_request_monitor.py --> has two jobs:
   a. looks for comments on the pull_request and parses them, if a comment uses a bang
   (i.e. '!') as the first character, the test suite of the given catalog is run using the dev branch
   b.  Checks to see if the PR is closed or merged. If closed, ends the job. If merged, merges the dev branch in Dremio 
   to main, by invoking the merge_catalog_branch function in **catalog_branch_manager.py**



Here's an image of a local copy of the CodeCommit repo:
![Screenshot 2024-05-02 at 1.27.46 AM.png](..%2F..%2F..%2F..%2F..%2F..%2FDesktop%2FScreenshot%202024-05-02%20at%201.27.46%20AM.png)

Here's an image of Dremio Cloud Catalog:


![Screenshot 2024-05-02 at 1.26.45 AM.png](..%2F..%2F..%2F..%2F..%2F..%2FDesktop%2FScreenshot%202024-05-02%20at%201.26.45%20AM.png)
