from squark.sql.dialects.base import BaseDialect


class Dialect(BaseDialect):
    engine = 'derby'

    def schema_is_writable(self, schema):
        return True