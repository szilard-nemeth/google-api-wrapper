import logging
import os
import sys
import unittest

from pythoncommons.file_utils import FileUtils, FindResultType


LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)
REPO_ROOT_DIRNAME = "google-api-wrapper"


class LocalDirs:
    REPO_ROOT_DIR = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)


class GoogleDriveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._setup_logging()
        cls.repo_root_dir = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def setUp(self):
        self.test_instance = self

    def tearDown(self) -> None:
        pass

    @classmethod
    def _ensure_env_var_is_present(cls, env_name):
        if env_name not in os.environ:
            raise ValueError(f"Please set '{env_name}' env var and re-run the test!")

    @classmethod
    def _setup_logging(cls):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        handler = logging.StreamHandler(stream=sys.stdout)
        CMD_LOG.propagate = False
        CMD_LOG.addHandler(handler)
        handler.setFormatter(logging.Formatter("%(message)s"))

    def test_x(self):
        print("Hello world")

    def test_y(self):
        pass
