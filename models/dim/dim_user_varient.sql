WITH src_user_varient AS (
    SELECT * FROM {{ ref('src_user_varient') }}
)
SELECT
    user_id,
    varient_id
FROM 
    src_user_varient