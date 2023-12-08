{% macro extrica__type_float() -%}
    double
{%- endmacro %}

{% macro extrica__type_string() -%}
    varchar
{%- endmacro %}

{% macro extrica__type_numeric() -%}
    decimal(28, 6)
{%- endmacro %}

{%- macro extrica__type_int() -%}
    integer
{%- endmacro -%}
