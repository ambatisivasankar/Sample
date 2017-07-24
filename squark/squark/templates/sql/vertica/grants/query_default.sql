SELECT * FROM GRANTS WHERE 1
  {% if grantee %}and GRANTEE = '{{grantee}}'{% endif %}
  {% if object_type %}and OBJECT_TYPE = '{{object_type.upper()}}'{% endif %}
  {% if object_name %}and OBJECT_NAME = '{{object_name}}'{% endif %}
  {% if permissions %}and PRIVILEGES_DESCRIPTION = '{{permissions|join(", ")}}' {% endif %}
  ;