
from abc import ABC, abstractmethod
from typing import Any


class AbstractDataTest(ABC):
    @abstractmethod
    def get_results(self) -> Any:
        pass

    @abstractmethod
    def check_result(self) -> bool:
        pass


class ScalarDataTest(AbstractDataTest):

    def get_results(self) -> Any:
        pass

    @abstractmethod
    def check_result(self) -> bool:
        return True


class DataTestFactory:
    def __init__(self, test_config):
        self.test_config = test_config
        self.type = 'type'
        return

    def create_data_test_object(self) -> AbstractDataTest:
        if self.test_config[self.type] == '':
            pass
        elif self.test_config[self.type] == '':
            pass
        else:
            pass
        return


