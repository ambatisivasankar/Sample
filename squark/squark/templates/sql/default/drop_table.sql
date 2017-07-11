DROP TABLE {% include "table_name.j2" %}{% if force %} CASCADE{% endif %};
