REVOKE {{ permissions | join(",") }}
  {% if revoke_on_all %}
  ON  ALL {{ object_type|upper }}S IN {% include "schema_name.j2" %}
  {% else %}
  ON  {{ object_type }} {% include "object_name.j2" %}
  {% endif %}
  FROM {{ grantee }}
  {% if force %} CASCADE{% endif %};
