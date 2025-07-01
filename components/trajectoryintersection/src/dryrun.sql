CREATE OR REPLACE TEMP FUNCTION _TRAJECTORYINTERSECTION_DRYRUN_QUERY(
    INPUT_TABLE STRING,
    INPUT_TABLE_POLYGON STRING,
    TPOINTS_COL STRING,
    RETURN_POLYGON_PROPERTIES BOOLEAN,
    POLYGON_KEY_COL STRING,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    // Build the select clause for columns
    let selectColumns;
    if (RETURN_POLYGON_PROPERTIES && POLYGON_KEY_COL) {
        selectColumns = `t.*, p.* EXCLUDE (${POLYGON_KEY_COL})`;
    } else if (RETURN_POLYGON_PROPERTIES) {
        selectColumns = 't.*, p.*';
    } else {
        selectColumns = 't.*';
    }

    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            WITH
                traj_cte AS (
                    SELECT * FROM ${INPUT_TABLE} LIMIT 1
                ),
                polygon_cte AS (
                    SELECT * FROM ${INPUT_TABLE_POLYGON} LIMIT 1
                )
            SELECT
                ${selectColumns}
            FROM
                traj_cte t CROSS JOIN polygon_cte p
            WHERE FALSE
        )
    `;
$$;

EXECUTE IMMEDIATE _TRAJECTORYINTERSECTION_DRYRUN_QUERY(
    :input_table,
    :input_table_polygon,
    :tpoints_col,
    :return_polygon_properties,
    :polygon_key_col,
    :output_table
);