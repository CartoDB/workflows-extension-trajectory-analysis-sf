CREATE OR REPLACE TEMP FUNCTION _STOPDETECTOR_QUERY(
    INPUT_TABLE STRING,
    TRAJ_ID_COL STRING,
    TPOINTS_COL STRING,
    METHOD STRING,
    MIN_DURATION FLOAT,
    DURATION_UNIT STRING,
    MAX_DIAMETER FLOAT,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    if (METHOD === 'Points') {
        return `
            CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
                SELECT
                    ${TRAJ_ID_COL},
                    p.value:stop_id::STRING as stop_id,
                    ST_GEOGRAPHYFROMWKT(p.value:geometry::STRING) AS geom,
                    p.value:start_time::TIMESTAMP as start_time,
                    p.value:end_time::TIMESTAMP as end_time,
                    p.value:duration_s::FLOAT as duration_s
                FROM ${INPUT_TABLE},
                LATERAL FLATTEN(input => @@workflows_temp@@.TRAJECTORY_STOP_POINTS(
                    ${TRAJ_ID_COL},
                    ${TPOINTS_COL},
                    ${MAX_DIAMETER},
                    ${MIN_DURATION},
                    '${DURATION_UNIT}'
                )) AS p
                ORDER BY ${TRAJ_ID_COL}, p.value:stop_id::STRING
            )
        `;
    } else {
        return `
            CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
                SELECT
                    ${TRAJ_ID_COL},
                    p.value:stop_id::STRING as stop_id,
                    ARRAY_AGG(
                        OBJECT_CONSTRUCT(
                            'lon', p.value:lon::FLOAT,
                            'lat', p.value:lat::FLOAT,
                            't', REPLACE(p.value:t::STRING, ' UTC', '')::TIMESTAMP,
                            'properties', p.value:properties::STRING
                        )
                    ) WITHIN GROUP (ORDER BY REPLACE(p.value:t::STRING, ' UTC', '')::TIMESTAMP) AS tpoints
                FROM ${INPUT_TABLE},
                LATERAL FLATTEN(input => @@workflows_temp@@.TRAJECTORY_STOP_SEGMENTS(
                    ${TRAJ_ID_COL},
                    ${TPOINTS_COL},
                    ${MAX_DIAMETER},
                    ${MIN_DURATION},
                    '${DURATION_UNIT}'
                )) AS p
                GROUP BY ${TRAJ_ID_COL}, p.value:stop_id::STRING
                ORDER BY ${TRAJ_ID_COL}, p.value:stop_id::STRING
            )
        `;
    }
$$;

EXECUTE IMMEDIATE _STOPDETECTOR_QUERY(
    :input_table,
    :traj_id_col,
    :tpoints_col,
    :method,
    :min_duration,
    :duration_unit,
    :max_diameter,
    :output_table
);