{% macro get_yoy_growth(quarter_col, year_col, revenue_col, service_type_col, service_type_val, table_name)  %} 
    SELECT {{quarter_col}}, {{year_col}}, {{service_type_col}},
    CONCAT(ROUND({{revenue_col}} / LAG({{revenue_col}}) OVER(PARTITION BY {{quarter_col}} ORDER BY {{year_col}}),2), '%') AS yoy_growth
    FROM {{table_name}}
    WHERE {{service_type_col}} = '{{service_type_val}}'
{%endmacro%}