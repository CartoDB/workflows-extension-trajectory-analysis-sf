CREATE OR REPLACE TEMP FUNCTION _GET_VALUES_AT_TIMESTAMP_QUERY(
    INPUT_TABLE STRING,
    TRAJ_ID_COL STRING,
    TPOINTS_COL STRING,
    TIMESTAMP_STR STRING,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            WITH result AS (
                SELECT
                    ${TRAJ_ID_COL},
                    @@workflows_temp@@.GET_VALUES_AT_TIMESTAMP(
                        ${TRAJ_ID_COL},
                        ${TPOINTS_COL},
                        '${TIMESTAMP_STR}'
                    ) AS values_at_timestamp
                FROM ${INPUT_TABLE}
                WHERE @@workflows_temp@@.GET_VALUES_AT_TIMESTAMP(
                    ${TRAJ_ID_COL},
                    ${TPOINTS_COL},
                    '${TIMESTAMP_STR}'
                ) IS NOT NULL
            )
            SELECT
                ${TRAJ_ID_COL},
                values_at_timestamp:t::TIMESTAMP AS t,
                ST_GEOGRAPHYFROMWKT(values_at_timestamp:geom::STRING) AS geom
            FROM result
        )
    `;
$$;

EXECUTE IMMEDIATE _GET_VALUES_AT_TIMESTAMP_QUERY(
    :input_table,
    :traj_id_col,
    :tpoints_col,
    :timestamp,
    :output_table
);
