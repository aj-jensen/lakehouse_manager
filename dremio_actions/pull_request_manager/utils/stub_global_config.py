
from .global_config import GlobalConfig


class StubGlobalConfig(GlobalConfig):
    project_id: str = ''
    catalog_id: str = ''
    dremio_cloud_region: str = ''
    repo_name: str = ''
    from_ref: str = ''
    to_ref: str = ''
    pull_request_id: str = ''
    # should be the path from repo root to the folder that mirrors the catalog, in case of entire repo mirroring, the
    # catalog, it's just the repo name:
    path_from_codecommit_root_to_catalog_mirror: str = ''
    dremio_cloud_secret: str = ''
    path_to_tests: str = ''
    glue_job_runtime_upper_bound_seconds: int = 172800  # 48hours*(60min)*(60seconds)