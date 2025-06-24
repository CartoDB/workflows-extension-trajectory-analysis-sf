CREATE OR REPLACE TEMP FUNCTION _DRYRUN_QUERY(
    OUTPUT_TABLE STRING,
    TRAJ_ID_COLUMN STRING,
    TPOINTS_COLUMN STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS
        SELECT
            '' AS ${TRAJ_ID_COLUMN},
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'lon', 0.0,
                    'lat', 0.0,
                    't', CURRENT_TIMESTAMP(),
                    'properties', '{}'
                )
            ) AS ${TPOINTS_COLUMN}
        WHERE FALSE
    `;
$$;

EXECUTE IMMEDIATE _DRYRUN_QUERY(
    :output_table,
    :input_traj_id_column,
    :input_tpoints_column
);
