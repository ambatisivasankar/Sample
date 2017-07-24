SELECT * FROM
  {% if schema %}{% include "schema_name.j2" %}{% endif %}{% include "table_name.j2" %}
  WHERE 1=0;