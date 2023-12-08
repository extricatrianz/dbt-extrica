{% macro extrica__any_value(expression) -%}
    min({{ expression }})
{%- endmacro %}
