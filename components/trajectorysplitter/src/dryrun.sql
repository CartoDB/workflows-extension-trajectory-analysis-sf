CREATE OR REPLACE TEMP FUNCTION _DRYRUN_QUERY(
    INPUT_TABLE STRING,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            SELECT
                input.*,
                '' AS seg_id
            FROM
                ${INPUT_TABLE} input
            WHERE FALSE
        )
    `;
$$;

EXECUTE IMMEDIATE _DRYRUN_QUERY(
    :input_table,
    :output_table
);
