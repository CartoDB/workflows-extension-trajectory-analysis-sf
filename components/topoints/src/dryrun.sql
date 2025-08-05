CREATE OR REPLACE TEMP FUNCTION _TOPOINTS_DRYRUN_QUERY(
    INPUT_TRAJ_ID_COLUMN STRING,
    OUTPUT_LINES BOOLEAN,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    if (!OUTPUT_LINES) {
        return `
            CREATE OR REPLACE TABLE ${OUTPUT_TABLE} (
                ${INPUT_TRAJ_ID_COLUMN} VARCHAR,
                geom GEOGRAPHY,
                t DATETIME,
                properties VARCHAR
            ) AS (
                SELECT * FROM VALUES 
                (NULL, NULL, NULL, NULL)
                WHERE FALSE
            )
        `;
    } else {
        return `
            CREATE OR REPLACE TABLE ${OUTPUT_TABLE} (
                ${INPUT_TRAJ_ID_COLUMN} VARCHAR,
                geom_start GEOGRAPHY,
                t_start DATETIME,
                properties_start VARCHAR,
                geom_end GEOGRAPHY,
                t_end DATETIME,
                properties_end VARCHAR,
                geom GEOGRAPHY
            ) AS (
                SELECT * FROM VALUES 
                (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                WHERE FALSE
            )
        `;
    }
$$;

EXECUTE IMMEDIATE _TOPOINTS_DRYRUN_QUERY(
    :input_traj_id_column,
    :output_lines,
    :output_table
);