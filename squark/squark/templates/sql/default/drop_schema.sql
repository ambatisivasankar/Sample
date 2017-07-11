DROP SCHEMA {% include "schema_name.j2" %}{% if force %} CASCADE{% endif %};
