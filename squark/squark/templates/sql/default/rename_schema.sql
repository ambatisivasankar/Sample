{% with schema=src_schema %}
  ALTER SCHEMA  {% include "schema_name.j2" %} RENAME TO
{% endwith %} 
{% with schema=dest_schema %}
  {% include "schema_name.j2" %}
{% endwith %};
