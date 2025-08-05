CREATE OR REPLACE TEMP FUNCTION _TOPOINTS_QUERY(
    INPUT_TABLE STRING,
    INPUT_TRAJ_ID_COLUMN STRING,
    INPUT_TPOINTS_COLUMN STRING,
    OUTPUT_LINES BOOLEAN,
    OUTPUT_TABLE STRING
)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    if (!OUTPUT_LINES) {
        return `
            CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
                WITH CTE AS (
                    SELECT *
                    FROM ${INPUT_TABLE}
                ),
                unnested_points AS (
                    SELECT 
                        ${INPUT_TRAJ_ID_COLUMN} AS ${INPUT_TRAJ_ID_COLUMN}, 
                        ST_MAKEPOINT(p.value:lon::FLOAT, p.value:lat::FLOAT) AS geom,
                        REPLACE(p.value:t::STRING, ' UTC', '')::DATETIME AS t,
                        p.value:properties::STRING AS properties
                    FROM CTE,
                    LATERAL FLATTEN(input => ${INPUT_TPOINTS_COLUMN}) AS p
                )
                SELECT * FROM unnested_points ORDER BY ${INPUT_TRAJ_ID_COLUMN}, t
            )
        `;
    } else {
        return `
            CREATE OR REPLACE TABLE ${OUTPUT_TABLE} AS (
                WITH CTE AS (
                    SELECT *
                    FROM ${INPUT_TABLE}
                ),
                unnested_points AS (
                    SELECT 
                        ${INPUT_TRAJ_ID_COLUMN} AS ${INPUT_TRAJ_ID_COLUMN}, 
                        ST_MAKEPOINT(p.value:lon::FLOAT, p.value:lat::FLOAT) AS geom,
                        REPLACE(p.value:t::STRING, ' UTC', '')::DATETIME AS t,
                        p.value:properties::STRING AS properties
                    FROM CTE,
                    LATERAL FLATTEN(input => ${INPUT_TPOINTS_COLUMN}) AS p
                ),
                numbered_points AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY ${INPUT_TRAJ_ID_COLUMN} ORDER BY t) AS rn
                    FROM unnested_points
                ),
                segments AS (
                    SELECT 
                        p1.${INPUT_TRAJ_ID_COLUMN},
                        p1.geom AS geom_start,
                        p1.t AS t_start,
                        p1.properties AS properties_start,
                        p2.geom AS geom_end,
                        p2.t AS t_end,
                        p2.properties AS properties_end,
                        ST_MAKELINE(p1.geom, p2.geom) AS geom
                    FROM numbered_points p1
                    JOIN numbered_points p2
                    ON p1.${INPUT_TRAJ_ID_COLUMN} = p2.${INPUT_TRAJ_ID_COLUMN} AND p1.rn + 1 = p2.rn
                )
                SELECT * FROM segments ORDER BY ${INPUT_TRAJ_ID_COLUMN}, t_start
            )
        `;
    }
$$;

EXECUTE IMMEDIATE _TOPOINTS_QUERY(
    :input_table,
    :input_traj_id_column,
    :input_tpoints_column,
    :output_lines,
    :output_table
);