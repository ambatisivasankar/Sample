GRANT {{ permissions | join(",") }}
  ON {{ object_type }} {% include "schema_name.j2" %}
  TO {{ grantee }}
  {% if with_grant %} WITH GRANT OPTION{% endif %};