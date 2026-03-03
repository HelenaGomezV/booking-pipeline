{% macro test_positive_value(model, column_name) %}
SELECT 
    {{ column_name }} AS invalid_value, 
    COUNT(*) AS occurrences
FROM {{ model }}
WHERE {{ column_name }} < 0
GROUP BY {{ column_name }}
HAVING COUNT(*) > 0
{% endmacro %}
