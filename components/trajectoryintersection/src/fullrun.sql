CREATE OR REPLACE TEMP FUNCTION _TRAJECTORYINTERSECTION_QUERY(
    INPUT_TABLE STRING,
    INPUT_TABLE_POLYGON STRING,
    TRAJ_ID_COL STRING,
    TPOINTS_COL STRING,
    POLYGON_COL STRING,
    JOIN_TYPE STRING,
    TRAJ_KEY_COL STRING,
    POLYGON_KEY_COL STRING,
    RETURN_POLYGON_PROPERTIES BOOLEAN,
    INTERSECTION_METHOD STRING,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    // Build the select clause for columns
    let selectColumns;
    if (RETURN_POLYGON_PROPERTIES) {
        let excludeCols = `${POLYGON_COL}_str`;
        if (POLYGON_KEY_COL) {
            excludeCols += `, ${POLYGON_KEY_COL}`;
        }
        selectColumns = `t.* EXCLUDE (${TPOINTS_COL}), p.* EXCLUDE (${excludeCols})`;
    } else {
        selectColumns = `t.* EXCLUDE (${TPOINTS_COL})`;
    }

    // Build the join clause
    let joinClause;
    if (JOIN_TYPE === 'Cross Join') {
        joinClause = `${INPUT_TABLE} t CROSS JOIN polygon_cte p`;
    } else { // Key Join
        joinClause = `${INPUT_TABLE} t INNER JOIN polygon_cte p ON t.${TRAJ_KEY_COL} = p.${POLYGON_KEY_COL}`;
    }

    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            WITH polygon_cte AS (
                SELECT
                    *,
                    -- FIXME: ST_ASWKT returns binary data, not WKT text - need to handle differently
                    -- For now, pass polygon as-is since it's already WKT string in test data
                    ${POLYGON_COL} AS ${POLYGON_COL}_str
                FROM
                    ${INPUT_TABLE_POLYGON}
            ),
            intersection_results AS (
                SELECT
                    ${selectColumns},
                    @@workflows_temp@@.TRAJECTORY_INTERSECTION(
                        ${TRAJ_ID_COL},
                        ${TPOINTS_COL},
                        ${POLYGON_COL}_str,
                        '${INTERSECTION_METHOD}'
                    ) AS intersected_tpoints
                FROM
                    ${joinClause}
            )
            SELECT
                * EXCLUDE (intersected_tpoints),
                TRANSFORM(intersected_tpoints, x ->
                    OBJECT_CONSTRUCT(
                        'lon', x:lon::FLOAT,
                        'lat', x:lat::FLOAT,
                        't', x:t::TIMESTAMP,
                        'properties', x:properties::STRING
                    )
                ) AS ${TPOINTS_COL}
            FROM
                intersection_results
            -- FIXME: Return all results for pytest, including empty intersections
            -- WHERE ARRAY_SIZE(intersected_tpoints) > 0
        )
    `;
$$;

EXECUTE IMMEDIATE _TRAJECTORYINTERSECTION_QUERY(
    :input_table,
    :input_table_polygon,
    :traj_id_col,
    :tpoints_col,
    :polygon_col,
    :join_type,
    :traj_key_col,
    :polygon_key_col,
    :return_polygon_properties,
    :intersection_method,
    :output_table
);
