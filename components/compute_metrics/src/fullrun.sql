CREATE OR REPLACE TEMP FUNCTION _COMPUTE_METRICS_QUERY(
    INPUT_TABLE STRING,
    INPUT_TRAJ_ID_COLUMN STRING,
    INPUT_TPOINTS_COLUMN STRING,
    INPUT_DISTANCE_BOOL BOOLEAN,
    INPUT_DURATION_BOOL BOOLEAN,
    INPUT_DIRECTION_BOOL BOOLEAN,
    INPUT_SPEED_BOOL BOOLEAN,
    INPUT_ACCELERATION_BOOL BOOLEAN,
    INPUT_DISTANCE_COLUMN STRING,
    INPUT_DURATION_COLUMN STRING,
    INPUT_DIRECTION_COLUMN STRING,
    INPUT_SPEED_COLUMN STRING,
    INPUT_ACCELERATION_COLUMN STRING,
    INPUT_DISTANCE_UNIT_DISTANCE STRING,
    INPUT_SPEED_UNIT_DISTANCE STRING,
    INPUT_ACCELERATION_UNIT_DISTANCE STRING,
    INPUT_DURATION_UNIT_TIME STRING,
    INPUT_SPEED_UNIT_TIME STRING,
    INPUT_ACCELERATION_UNIT_TIME STRING,
    OUTPUT_TABLE STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
CALLED ON NULL INPUT
AS
$$
    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            WITH metrics_cte AS (
                SELECT
                    ${INPUT_TRAJ_ID_COLUMN},
                    ARRAY_AGG(
                        OBJECT_CONSTRUCT(
                            'lon', p.value:lon::FLOAT,
                            'lat', p.value:lat::FLOAT,
                            't', REPLACE(p.value:t::STRING, ' UTC', '')::DATETIME,
                            'properties', p.value:properties::STRING
                        )
                    ) WITHIN GROUP (ORDER BY REPLACE(p.value:t::STRING, ' UTC', '')::DATETIME) AS tpoints
                FROM ${INPUT_TABLE},
                LATERAL FLATTEN(input => @@workflows_temp@@.TRAJECTORY_METRICS(
                    ${INPUT_TRAJ_ID_COLUMN},
                    ${INPUT_TPOINTS_COLUMN},
                    ${INPUT_DISTANCE_BOOL},
                    ${INPUT_DURATION_BOOL},
                    ${INPUT_DIRECTION_BOOL},
                    ${INPUT_SPEED_BOOL},
                    ${INPUT_ACCELERATION_BOOL},
                    '${INPUT_DISTANCE_COLUMN}',
                    '${INPUT_DURATION_COLUMN}',
                    '${INPUT_DIRECTION_COLUMN}',
                    '${INPUT_SPEED_COLUMN}',
                    '${INPUT_ACCELERATION_COLUMN}',
                    '${INPUT_DISTANCE_UNIT_DISTANCE}',
                    '${INPUT_SPEED_UNIT_DISTANCE}',
                    '${INPUT_ACCELERATION_UNIT_DISTANCE}',
                    '${INPUT_DURATION_UNIT_TIME}',
                    '${INPUT_SPEED_UNIT_TIME}',
                    '${INPUT_ACCELERATION_UNIT_TIME}'
                )) AS p
                GROUP BY ${INPUT_TRAJ_ID_COLUMN}
            )
            SELECT
                input.* EXCLUDE (${INPUT_TPOINTS_COLUMN}),
                metrics.tpoints AS ${INPUT_TPOINTS_COLUMN}
            FROM
                ${INPUT_TABLE} input
            INNER JOIN
                metrics_cte metrics
            ON input.${INPUT_TRAJ_ID_COLUMN} = metrics.${INPUT_TRAJ_ID_COLUMN}
            ORDER BY input.${INPUT_TRAJ_ID_COLUMN}
        )
    `;
$$;

EXECUTE IMMEDIATE _COMPUTE_METRICS_QUERY(
    :input_table,
    :input_traj_id_column,
    :input_tpoints_column,
    :input_distance_bool,
    :input_duration_bool,
    :input_direction_bool,
    :input_speed_bool,
    :input_acceleration_bool,
    :input_distance_column,
    :input_duration_column,
    :input_direction_column,
    :input_speed_column,
    :input_acceleration_column,
    :input_distance_unit_distance,
    :input_speed_unit_distance,
    :input_acceleration_unit_distance,
    :input_duration_unit_time,
    :input_speed_unit_time,
    :input_acceleration_unit_time,
    :output_table
);
