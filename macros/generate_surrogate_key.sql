-- macros/generate_surrogate_key.sql

{#
    Đây là một macro để tạo ra các khóa thay thế (surrogate key)
    bằng cách ghép platform_id với một ID gốc của nền tảng.

    Đối số (Arguments):
    - platform_id_column: Tên cột chứa platform_id (ví dụ: 'platform_id')
    - source_id_column: Tên cột chứa ID gốc (ví dụ: 'platform_account_id')
#}

{% macro generate_surrogate_key(platform_id_column, source_id_column) %}
    concat(toString({{ platform_id_column }}), '__', {{ source_id_column }})
{% endmacro %}