
class ConfigError(Exception):
    pass


class ConfigNotFoundError(ConfigError, FileNotFoundError):
    pass


class SquarkInvocationError(Exception):
    pass


class LocationNotWritable(Exception):
    pass


class LocationAlreadyExists(Exception):
    pass