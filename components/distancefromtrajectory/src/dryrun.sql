CREATE OR REPLACE TEMP FUNCTION _DISTANCE_FROM_TRAJECTORY_DRYRUN_QUERY(
    INPUT_TABLE STRING,
    INPUT_TABLE_POSITION STRING,
    RETURN_POSITION_PROPERTIES BOOLEAN,
    POSITION_KEY_COL STRING,
    DISTANCE_OUTPUT_COL STRING,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    const selectClause = RETURN_POSITION_PROPERTIES && POSITION_KEY_COL ? 
        `t.*, p.* EXCLUDE (${POSITION_KEY_COL})` :
        RETURN_POSITION_PROPERTIES ? 
        `t.*, p.*` :
        `t.*`;

    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            WITH
                traj_cte AS (
                    SELECT * FROM ${INPUT_TABLE} LIMIT 1
                ),
                position_cte AS (
                    SELECT * FROM ${INPUT_TABLE_POSITION} LIMIT 1
                )
            SELECT
                ${selectClause},
                0.0 AS ${DISTANCE_OUTPUT_COL}
            FROM
                traj_cte t CROSS JOIN position_cte p
            WHERE FALSE
        )
    `;
$$;

EXECUTE IMMEDIATE _DISTANCE_FROM_TRAJECTORY_DRYRUN_QUERY(
    :input_table,
    :input_table_position,
    :return_position_properties,
    :position_key_col,
    :distance_output_col,
    :output_table
);
