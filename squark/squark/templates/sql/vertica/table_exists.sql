SELECT * FROM V_CATALOG.TABLES WHERE
  TABLE_NAME = '{{table}}'{% if not schema %};
  {% else %} and TABLE_SCHEMA = '{{schema}}';{% endif %}
