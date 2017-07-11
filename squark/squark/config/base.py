import logging


class ConfigObj:

    def __init__(self, cfg):
        self.cfg = cfg
        self.logger = logging.getLogger('squark')

    # Much easier to implement this on a non-dict than
    # the other way around...
    __getitem__ = object.__getattr__
