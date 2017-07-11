create table if not exists {% include "object_name.j2" %}(
{% for col in columns %}
    {{ col.name }} {{ col.ddl }}{% if not col.nullable %} NOT NULL{% endif %},
{% endfor %}
{% include "extra_create_table_clauses.j2" %}
);
