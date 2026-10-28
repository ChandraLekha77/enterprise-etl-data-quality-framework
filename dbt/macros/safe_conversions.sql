{% macro safe_text(expression) -%}
    nullif(btrim({{ expression }}::text), '')
{%- endmacro %}

{% macro safe_timestamptz(expression) -%}
    case
        when {{ expression }} is null then null
        when pg_input_is_valid(btrim({{ expression }}::text), 'timestamp with time zone')
          then btrim({{ expression }}::text)::timestamptz
        else null
    end
{%- endmacro %}

{% macro safe_numeric(expression, precision=18, scale=2) -%}
    case
        when pg_input_is_valid(btrim({{ expression }}::text), 'numeric')
          then round(btrim({{ expression }}::text)::numeric, {{ scale }})
        else null
    end
{%- endmacro %}
