CREATE USER {{user}}
  {% if account %}ACCOUNT {{account}}{% endif %}
  {% if identified %}IDENTIFIED BY {{identified}}{% endif %}
  {% if memorycap %}MEMORYCAP {{memorycap}}{% endif %}
  {% if password %}PASSWORD EXPIRE  {{password}}{% endif %}
  {% if profile %}PROFILE  {{profile}}{% endif %}
  {% if resource_pool %}RESOURCE POOL {{resource_pool}}{% endif %}
  {% if runtimecap %}RUNTIMECAP  {{runtimecap}}{% endif %}
  {% if tempspacecap %}TEMPSPACECAP {{tempspacecap}}{% endif %}
  {% if search_path %}SEARCH_PATH {{search_path}}{% endif %}
;