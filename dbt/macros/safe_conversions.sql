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
         and abs(btrim({{ expression }}::text)::numeric) < power(10::numeric, {{ precision - scale }})
          then round(btrim({{ expression }}::text)::numeric, {{ scale }})::numeric({{ precision }}, {{ scale }})
        else null
    end
{%- endmacro %}
