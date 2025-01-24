{% macro handle_invalid_time(time_column) %}
    CAST(
        CASE 
            WHEN CAST(SUBSTRING({{ time_column }}, 1, 2) AS INT64) >= 24 THEN
                CONCAT(
                    LPAD(CAST(CAST(SUBSTRING({{ time_column }}, 1, 2) AS INT64) - 24 AS STRING), 2, '0'),
                    SUBSTRING({{ time_column }}, 3)
                )

            ELSE
                {{ time_column }}
        END
    AS TIME)
{% endmacro %}