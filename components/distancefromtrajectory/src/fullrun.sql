CREATE OR REPLACE TEMP FUNCTION _DISTANCE_FROM_TRAJECTORY_QUERY(
    INPUT_TABLE STRING,
    INPUT_TABLE_POSITION STRING,
    TRAJ_ID_COL STRING,
    TPOINTS_COL STRING,
    JOIN_TYPE STRING,
    TRAJ_KEY_COL STRING,
    POSITION_KEY_COL STRING,
    POSITION_COL STRING,
    RETURN_POSITION_PROPERTIES BOOLEAN,
    DISTANCE_FROM STRING,
    UNITS STRING,
    DISTANCE_OUTPUT_COL STRING,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    const positionSelectClause = RETURN_POSITION_PROPERTIES && POSITION_KEY_COL ? 
        `t.*, p.* EXCLUDE (${POSITION_COL}_str, ${POSITION_KEY_COL})` :
        RETURN_POSITION_PROPERTIES ? 
        `t.*, p.* EXCLUDE (${POSITION_COL}_str)` :
        `t.*`;
    
    const joinClause = JOIN_TYPE === 'Cross Join' ?
        `${INPUT_TABLE} t CROSS JOIN position_cte p` :
        `${INPUT_TABLE} t INNER JOIN position_cte p ON t.${TRAJ_KEY_COL} = p.${POSITION_KEY_COL}`;

    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            WITH position_cte AS (
                SELECT
                    *,
                    ST_ASWKT(${POSITION_COL}) AS ${POSITION_COL}_str
                FROM
                    ${INPUT_TABLE_POSITION}
            )
            SELECT
                ${positionSelectClause},
                @@workflows_temp@@.DISTANCE_FROM_TRAJECTORY(
                    ${TRAJ_ID_COL},
                    ${TPOINTS_COL},
                    ${POSITION_COL}_str,
                    '${DISTANCE_FROM}',
                    '${UNITS}'
                ) AS ${DISTANCE_OUTPUT_COL}
            FROM
                ${joinClause}
        )
    `;
$$;

EXECUTE IMMEDIATE _DISTANCE_FROM_TRAJECTORY_QUERY(
    :input_table,
    :input_table_position,
    :traj_id_col,
    :tpoints_col,
    :join_type,
    :traj_key_col,
    :position_key_col,
    :position_col,
    :return_position_properties,
    :distance_from,
    :units,
    :distance_output_col,
    :output_table
);
