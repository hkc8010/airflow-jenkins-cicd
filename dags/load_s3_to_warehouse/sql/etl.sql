BEGIN;
{% if params.delsert_column %}
        DELETE  FROM {{ params.dest_schema }}.{{ params.dest_table }}
        WHERE   1 = 1
        AND {{ params.delsert_filter }}
        AND {{ params.dest_schema }}.{{ params.dest_table }}.{{ params.delsert_column }} IN (
                SELECT {{ params.staging_schema }}.{{ params.staging_table }}.{{ params.delsert_column }} 
                FROM {{ params.staging_schema }}.{{ params.staging_table }});

{% elif params.upsert_id_columns_str %}
        DELETE  FROM {{ params.dest_schema }}.{{ params.dest_table }}
        USING   {{ params.staging_schema }}.{{ params.staging_table }}
        WHERE   1 = 1
        {% for id in params.upsert_id_columns_str.split(', ') %}
                AND {{ params.dest_schema }}.{{ params.dest_table }}.{{ id }} = {{ params.staging_schema }}.{{ params.staging_table }}.{{ id }}
        {% endfor %}
        {% if params.partition_column %}
                AND  {{ params.dest_schema }}.{{ params.dest_table }}.{{ params.partition_column }} >= (SELECT MIN(SUBSTRING({{ params.partition_column }},1,19)::timestamp) FROM {{ params.staging_schema }}.{{ params.staging_table }})
                AND  {{ params.dest_schema }}.{{ params.dest_table }}.{{ params.partition_column }} <= (SELECT MAX(SUBSTRING({{ params.partition_column }},1,19)::timestamp) FROM {{ params.staging_schema }}.{{ params.staging_table }})
        {% endif %};
{% endif %}

        INSERT INTO {{ params.dest_schema }}.{{ params.dest_table }}
        (
        {{ params.insert_columns_str }}
        )
        SELECT  {{ params.select_columns_str }}
{% if params.dedupe_order_by_columns_str %}
        FROM    (SELECT *,
                        ROW_NUMBER() OVER (
                                PARTITION BY {{ params.upsert_id_columns_str }} 
                                ORDER BY {{ params.dedupe_order_by_columns_str }} DESC) AS _sys_rank
                FROM   {{ params.staging_schema }}.{{ params.staging_table }} a
                ) a
        WHERE   _sys_rank = 1;
{% else %}
        FROM    {{ params.staging_schema }}.{{ params.staging_table }} a
        WHERE   1 = 1
{% endif %}
                AND  ({{ params.additional_filter }});


COMMIT;
