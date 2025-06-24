CREATE OR REPLACE TEMP FUNCTION _FROMPOINTS_QUERY(
    INPUT_TABLE STRING,
    TRAJ_ID_COLUMN STRING,
    GEOM_COLUMN STRING,
    T_COLUMN STRING,
    TPOINTS_COLUMN STRING,
    PROPERTIES_COLUMNS STRING,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    // Generate properties construction based on input columns
    let propertiesConstruct;
    if (!PROPERTIES_COLUMNS || PROPERTIES_COLUMNS.trim() === '') {
        propertiesConstruct = "'{}'";
    } else {
        const columns = PROPERTIES_COLUMNS.split(',').map(col => col.trim());
        const pairs = columns.map(col => `'${col}', ${col}`).join(', ');
        propertiesConstruct = `TO_JSON(OBJECT_CONSTRUCT(${pairs}))`;
    }

    return `
        CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
            SELECT
                ${TRAJ_ID_COLUMN},
                ARRAY_AGG(
                    OBJECT_CONSTRUCT(
                        'lon', ST_X(${GEOM_COLUMN}),
                        'lat', ST_Y(${GEOM_COLUMN}),
                        't', ${T_COLUMN},
                        'properties', ${propertiesConstruct}
                    )
                ) WITHIN GROUP (ORDER BY ${T_COLUMN}) AS ${TPOINTS_COLUMN}
            FROM ${INPUT_TABLE}
            GROUP BY ${TRAJ_ID_COLUMN}
        )
    `;
$$;

EXECUTE IMMEDIATE _FROMPOINTS_QUERY(
    :input_table,
    :input_traj_id_column,
    :input_geom_column,
    :input_t_column,
    :input_tpoints_column,
    :input_properties_columns,
    :output_table
);
