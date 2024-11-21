{{ 
    config(
        materialized = 'incremental',
        on_schema_change='fail' 
    )
}}
WITH src_user_event AS (
    SELECT * FROM {{ ref("src_user_event") }}
) 
SELECT
    user_id, 
    datestamp, 
    item_id, 
    clicked, 
    purchased, 
    paidamount
FROM 
    src_user_even
WHERE
    datestamp is not NULL
    {% if is_incremental() %}
        AND datestamp > (SELECT MAX(datestamp) from {{ this }})
    {% endif %}
-- 개발자가 incremental 하게 만들어줘야 한다!