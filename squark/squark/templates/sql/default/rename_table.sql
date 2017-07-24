{% with table=src_table, schema=src_schema %}
  ALTER TABLE {% include "table_name.j2" %} RENAME TO "{{dest_table}}";
{% endwith %} 
