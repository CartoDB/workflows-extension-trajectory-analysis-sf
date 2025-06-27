CREATE OR REPLACE TEMP FUNCTION _TRAJECTORYSIMPLIFIER_QUERY(
    INPUT_TABLE STRING,
    TRAJ_ID_COL STRING,
    TPOINTS_COL STRING,
    TOLERANCE FLOAT,
    ROUNDING_PRECISION FLOAT,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            WITH simplified_cte AS (
                SELECT
                    ${TRAJ_ID_COL},
                    ARRAY_AGG(
                        OBJECT_CONSTRUCT(
                            'lon', p.value:lon::FLOAT,
                            'lat', p.value:lat::FLOAT,
                            't', REPLACE(p.value:t::STRING, ' UTC', '')::TIMESTAMP,
                            'properties', p.value:properties::STRING
                        )
                    ) WITHIN GROUP (ORDER BY REPLACE(p.value:t::STRING, ' UTC', '')::TIMESTAMP) AS tpoints
                FROM ${INPUT_TABLE},
                LATERAL FLATTEN(input => @@workflows_temp@@.TRAJECTORY_SIMPLIFIER(
                    ${TRAJ_ID_COL},
                    ${TPOINTS_COL},
                    ${TOLERANCE},
                    ${ROUNDING_PRECISION}
                )) AS p
                GROUP BY ${TRAJ_ID_COL}
            )
            SELECT
                input.* EXCLUDE (${TPOINTS_COL}),
                simplified.tpoints AS ${TPOINTS_COL}
            FROM ${INPUT_TABLE} input
            INNER JOIN simplified_cte simplified
            ON input.${TRAJ_ID_COL} = simplified.${TRAJ_ID_COL}
        )
    `;
$$;

EXECUTE IMMEDIATE _TRAJECTORYSIMPLIFIER_QUERY(
    :input_table,
    :traj_id_col,
    :tpoints_col,
    :tolerance,
    :rounding_precision,
    :output_table
);
