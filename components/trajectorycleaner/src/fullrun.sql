CREATE OR REPLACE TEMP FUNCTION _TRAJECTORYCLEANER_QUERY(
    INPUT_TABLE STRING,
    TRAJ_ID_COL STRING,
    TPOINTS_COL STRING,
    SPEED_THRESHOLD FLOAT,
    INPUT_UNIT_DISTANCE STRING,
    INPUT_UNIT_TIME STRING,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            WITH cleaned_cte AS (
                SELECT
                    ${TRAJ_ID_COL},
                    ARRAY_AGG(
                        OBJECT_CONSTRUCT(
                            'lon', p.value:lon::FLOAT,
                            'lat', p.value:lat::FLOAT,
                            't', REPLACE(p.value:t::STRING, ' UTC', '')::DATETIME,
                            'properties', p.value:properties::STRING
                        )
                    ) WITHIN GROUP (ORDER BY REPLACE(p.value:t::STRING, ' UTC', '')::DATETIME) AS tpoints,
                    ANY_VALUE(p.value:logs::STRING) AS logs
                FROM ${INPUT_TABLE},
                LATERAL FLATTEN(input => @@workflows_temp@@.TRAJECTORY_CLEANER_OUTLIER(
                    ${TRAJ_ID_COL},
                    ${TPOINTS_COL},
                    ${SPEED_THRESHOLD},
                    '${INPUT_UNIT_DISTANCE}',
                    '${INPUT_UNIT_TIME}'
                )) AS p
                GROUP BY ${TRAJ_ID_COL}
            )
            SELECT
                input.* EXCLUDE (${TPOINTS_COL}),
                cleaned.tpoints AS ${TPOINTS_COL},
                cleaned.logs AS logs
            FROM ${INPUT_TABLE} input
            INNER JOIN cleaned_cte cleaned
            ON input.${TRAJ_ID_COL} = cleaned.${TRAJ_ID_COL}
        )
    `;
$$;

EXECUTE IMMEDIATE _TRAJECTORYCLEANER_QUERY(
    :input_table,
    :traj_id_col,
    :tpoints_col,
    :speed_threshold,
    :input_unit_distance,
    :input_unit_time,
    :output_table
);
