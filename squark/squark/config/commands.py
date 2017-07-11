from squark.utils.common import resolve_name
from squark.base import ConfigType


class Commands(ConfigType):
    def __init__(self, cfg):
        super().__init__(cfg)
        self._build()

    def _build(self):
        for section, sec_cfg in self.cfg.items():
            for cmd_name, import_path in sec_cfg.items():
                cmd = resolve_name(import_path)
                setattr(self, cmd_name, cmd)
