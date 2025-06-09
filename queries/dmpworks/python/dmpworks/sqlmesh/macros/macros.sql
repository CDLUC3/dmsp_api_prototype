{%- macro array_agg_distinct(col) -%}
    COALESCE(ARRAY_AGG(DISTINCT {{ col }}), [])
{%- endmacro -%}