GRANT {{ permissions | join(",") }}
  {% if grant_on_all %}
  ON  ALL {{ object_type|upper }}S IN {% include "schema_name.j2" %}
  {% else %}
  ON  {{ object_type }} {% include "object_name.j2" %}
  {% endif %}
  TO {{ grantee }}
  {% if with_grant %} WITH GRANT OPTION{% endif %};