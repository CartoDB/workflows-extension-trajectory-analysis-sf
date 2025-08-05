CREATE OR REPLACE TEMP FUNCTION _STOPDETECTOR_DRYRUN_QUERY(
    TRAJ_ID_COL STRING,
    TPOINTS_COL STRING,
    METHOD STRING,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    if (METHOD === 'Points') {
        return `
            CREATE OR REPLACE TABLE ${OUTPUT_TABLE} (
                ${TRAJ_ID_COL} VARCHAR,
                stop_id VARCHAR,
                geom GEOGRAPHY,
                start_time DATETIME,
                end_time DATETIME,
                duration_s FLOAT
            ) AS (
                SELECT * FROM VALUES 
                (NULL, NULL, NULL, NULL, NULL, NULL)
                WHERE FALSE
            )
        `;
    } else {
        return `
            CREATE OR REPLACE TABLE ${OUTPUT_TABLE} (
                ${TRAJ_ID_COL} VARCHAR,
                stop_id VARCHAR,
                ${TPOINTS_COL} VARIANT
            ) AS (
                SELECT * FROM VALUES 
                (NULL, NULL, NULL)
                WHERE FALSE
            )
        `;
    }
$$;

EXECUTE IMMEDIATE _STOPDETECTOR_DRYRUN_QUERY(
    :traj_id_col,
    :tpoints_col,
    :method,
    :output_table
);