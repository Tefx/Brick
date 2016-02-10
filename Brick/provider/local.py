import Brick.service.local as LS
from base import ProviderBase


class ProcessProvider(ProviderBase):
    _service_class_ = LS.ProcessService
    _config_ = {"local": {"cpu_scale": 1}}

    def calculate_price(self, service):
        return 0


class LXCProvider(ProviderBase):
    _service_class_ = LS.LXCService
    _config_ = {"tiny": {"cpu_scale": 1},
                "small": {"cpu_scale": 2}}

    def calculate_price(self, service):
        return 0
