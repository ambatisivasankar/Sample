GRANT {{ permissions | join(",") }}
  ON {{ object_type }} "{{object_name}}"
  TO {{ grantee }}
  {% if with_grant %} WITH GRANT OPTION{% endif %};
