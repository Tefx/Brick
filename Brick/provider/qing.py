from Brick.service.qing import QingService
from base import ProviderBase


class QingProvider(ProviderBase):
    _service_class_ = QingService
    _config_ = {"c1m1": {"cpu_scale": 1, "price": 0.1}}

    def calculate_price(self, service):
        p = self._config_[service.conf]["price"]
        t = service.finish_time - service.start_time
        return p / 3600 * t
