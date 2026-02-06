CREATE OR REPLACE TEMP FUNCTION _TRAJECTORYSPLITTER_QUERY(
    INPUT_TABLE STRING,
    TRAJ_ID_COL STRING,
    TPOINTS_COL STRING,
    METHOD STRING,
    MIN_DURATION FLOAT,
    DURATION_UNIT STRING,
    MAX_DIAMETER FLOAT,
    TIME_MODE STRING,
    MIN_SPEED FLOAT,
    MIN_ANGLE FLOAT,
    VALUECHANGE_COL STRING,
    MIN_LENGTH FLOAT,
    OUTPUT_TABLE STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
CALLED ON NULL INPUT
AS
$$
    // Generate the appropriate UDF call based on method
    let udfCall;
    if (METHOD === 'Stops') {
        udfCall = `@@workflows_temp@@.TRAJECTORY_SPLITTER_STOP(
            ${TRAJ_ID_COL},
            ${TPOINTS_COL},
            ${MIN_DURATION}, '${DURATION_UNIT}',
            ${MAX_DIAMETER},
            ${MIN_LENGTH}
        )`;
    } else if (METHOD === 'Temporal') {
        udfCall = `@@workflows_temp@@.TRAJECTORY_SPLITTER_TEMPORAL(
            ${TRAJ_ID_COL},
            ${TPOINTS_COL},
            '${TIME_MODE}',
            ${MIN_LENGTH}
        )`;
    } else if (METHOD === 'Speed') {
        udfCall = `@@workflows_temp@@.TRAJECTORY_SPLITTER_SPEED(
            ${TRAJ_ID_COL},
            ${TPOINTS_COL},
            ${MIN_SPEED},
            ${MIN_DURATION}, '${DURATION_UNIT}',
            ${MIN_LENGTH}
        )`;
    } else if (METHOD === 'Observation Gap') {
        udfCall = `@@workflows_temp@@.TRAJECTORY_SPLITTER_OBSERVATION(
            ${TRAJ_ID_COL},
            ${TPOINTS_COL},
            ${MIN_DURATION}, '${DURATION_UNIT}',
            ${MIN_LENGTH}
        )`;
    } else if (METHOD === 'Angle Change') {
        udfCall = `@@workflows_temp@@.TRAJECTORY_SPLITTER_ANGLECHANGE(
            ${TRAJ_ID_COL},
            ${TPOINTS_COL},
            ${MIN_ANGLE},
            ${MIN_SPEED},
            ${MIN_LENGTH}
        )`;
    } else {
        throw new Error(`Unknown method: ${METHOD}`);
    }

    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            WITH split_cte AS (
                SELECT
                    ${TRAJ_ID_COL},
                    p.value:seg_id::STRING as seg_id,
                    ARRAY_AGG(
                        OBJECT_CONSTRUCT(
                            'lon', p.value:lon::FLOAT,
                            'lat', p.value:lat::FLOAT,
                            't', REPLACE(p.value:t::STRING, ' UTC', '')::DATETIME,
                            'properties', p.value:properties::STRING
                        )
                    ) WITHIN GROUP (ORDER BY REPLACE(p.value:t::STRING, ' UTC', '')::DATETIME) AS tpoints
                FROM ${INPUT_TABLE},
                LATERAL FLATTEN(input => ${udfCall}) AS p
                GROUP BY ${TRAJ_ID_COL}, p.value:seg_id::STRING
            )
            SELECT
                input.* EXCLUDE (${TPOINTS_COL}),
                split.seg_id,
                split.tpoints AS ${TPOINTS_COL}
            FROM
                ${INPUT_TABLE} input
            INNER JOIN
                split_cte split
            ON input.${TRAJ_ID_COL} = split.${TRAJ_ID_COL}
            ORDER BY input.${TRAJ_ID_COL}, split.seg_id
        )
    `;
$$;

EXECUTE IMMEDIATE _TRAJECTORYSPLITTER_QUERY(
    :input_table,
    :traj_id_col,
    :tpoints_col,
    :method,
    :min_duration,
    :duration_unit,
    :max_diameter,
    :time_mode,
    :min_speed,
    :min_angle,
    :valuechange_col,
    :min_length,
    :output_table
);
