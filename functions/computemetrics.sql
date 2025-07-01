CREATE OR REPLACE FUNCTION @@workflows_temp@@.TRAJECTORY_METRICS(
    traj_id STRING,
    trajectory ARRAY,
    input_distance_bool BOOLEAN,
    input_duration_bool BOOLEAN,
    input_direction_bool BOOLEAN,
    input_speed_bool BOOLEAN,
    input_acceleration_bool BOOLEAN,
    input_distance_column STRING,
    input_duration_column STRING,
    input_direction_column STRING,
    input_speed_column STRING,
    input_acceleration_column STRING,
    input_distance_unit_distance STRING,
    input_speed_unit_distance STRING,
    input_acceleration_unit_distance STRING,
    input_duration_unit_time STRING,
    input_speed_unit_time STRING,
    input_acceleration_unit_time STRING
)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('numpy','pandas','geopandas','movingpandas','shapely')
HANDLER = 'main'
AS
$$
from datetime import timedelta

import numpy as np
import pandas as pd
import geopandas as gpd
import movingpandas as mpd
import json

def main(
    traj_id,
    trajectory,
    input_distance_bool,
    input_duration_bool,
    input_direction_bool,
    input_speed_bool,
    input_acceleration_bool,
    input_distance_column,
    input_duration_column,
    input_direction_column,
    input_speed_column,
    input_acceleration_column,
    input_distance_unit_distance,
    input_speed_unit_distance,
    input_acceleration_unit_distance,
    input_duration_unit_time,
    input_speed_unit_time,
    input_acceleration_unit_time
):
    # Unit mapping from English names to short names
    distance_units = {
        "Kilometers": "km",
        "Meters": "m",
        "Miles": "mi",
        "Nautical Miles": "nm"
    }

    time_units = {
        "Seconds": "s",
        "Minutes": "min",
        "Hours": "h",
        "Days": "d"
    }

    # Unit conversion factors from seconds
    time_conversions = {
        "Seconds": 1,
        "Minutes": 60,
        "Hours": 3600,
        "Days": 86400
    }
    # build the DataFrame
    df = pd.DataFrame.from_records(trajectory)
    
    if df.shape[0] == 0:
        return []
    
    # MANDATORY: Convert timestamp strings to datetime for MovingPandas compatibility
    df['t'] = pd.to_datetime(df['t'])

    # check input metrics
    input_metrics = (input_distance_bool, input_duration_bool, input_direction_bool, input_speed_bool, input_acceleration_bool)
    if not any(input_metrics):
        raise ValueError(f'Select at least one metric to compute')

    # check properties names
    input_metrics_names = (input_distance_column, input_duration_column, input_direction_column, input_speed_column, input_acceleration_column)
    col_names = list(pd.json_normalize(df['properties'].apply(lambda x : json.loads(x))).columns)
    dup_names = [x for x in input_metrics_names if x in col_names]
    if len(dup_names) > 0:
        raise ValueError(f'The following properties already exist: {dup_names}')

    def merge_json(row):
        properties_json = json.loads(row['properties']) if isinstance(row['properties'], str) else row['properties']
        other_fields = {
            input_distance_column: row.get(input_distance_column),
            input_duration_column: row.get(input_duration_column),
            input_direction_column: row.get(input_direction_column),
            input_speed_column: row.get(input_speed_column),
            input_acceleration_column: row.get(input_acceleration_column),
        }
         # Filter out empty (non-computed) metrics and "undefined" parameters
        other_fields = {key: value for key, value in other_fields.items() if key and key != "undefined"}

        # Merge properties JSON with the other fields
        return json.dumps({**properties_json, **other_fields})

    # Check if trajectory has enough unique timestamps for metric calculations
    if df.empty or df.t.nunique() <= 1:
        # Return the original trajectory with empty columns for computed metrics
        result = df.copy()
        added_columns = []
        
        if input_distance_bool and input_distance_column != "undefined":
            result[input_distance_column] = None  # Use None instead of np.nan for JSON serialization
            added_columns.append(input_distance_column)
        if input_duration_bool and input_duration_column != "undefined":
            result[input_duration_column] = None
            added_columns.append(input_duration_column)
        if input_direction_bool and input_direction_column != "undefined":
            result[input_direction_column] = None
            added_columns.append(input_direction_column)
        if input_speed_bool and input_speed_column != "undefined":
            result[input_speed_column] = None
            added_columns.append(input_speed_column)
        if input_acceleration_bool and input_acceleration_column != "undefined":
            result[input_acceleration_column] = None
            added_columns.append(input_acceleration_column)

        result['properties'] = result.apply(merge_json, axis=1)
        result = result[['lon', 'lat', 't', 'properties']]
        return result.to_dict(orient='records')

    # build the GeoDataFrame
    gdf = (
      gpd.GeoDataFrame(
        df[['t', 'properties']],
        geometry=gpd.points_from_xy(df.lon, df.lat),
        crs=4326
      )
      .set_index('t')
    )

    # build the Trajectory object
    traj = mpd.Trajectory(gdf, traj_id)

    # Keep track of which columns were actually added
    added_columns = []
    
    # TODO: When MovingPandas version is updated, these methods may return the trajectory object
    # instead of modifying in-place. If so, change back to: traj = traj.add_*() pattern
    if input_distance_bool and input_distance_column != "undefined":
        traj.add_distance(name=input_distance_column, units=distance_units[input_distance_unit_distance])
        added_columns.append(input_distance_column)
    if input_duration_bool and input_duration_column != "undefined":
        traj.add_timedelta(name=input_duration_column)
        added_columns.append(input_duration_column)
    if input_direction_bool and input_direction_column != "undefined":
        traj.add_direction(name=input_direction_column)
        added_columns.append(input_direction_column)
    if input_speed_bool and input_speed_column != "undefined":
        traj.add_speed(name=input_speed_column, units=(distance_units[input_speed_unit_distance], time_units[input_speed_unit_time]))
        added_columns.append(input_speed_column)
    if input_acceleration_bool and input_acceleration_column != "undefined":
        traj.add_acceleration(name=input_acceleration_column, units=(distance_units[input_acceleration_unit_distance], time_units[input_acceleration_unit_time], time_units[input_acceleration_unit_time]))
        added_columns.append(input_acceleration_column)

    result = traj.to_point_gdf().reset_index()
    result['lon'] = result.geometry.x.astype(np.float64)
    result['lat'] = result.geometry.y.astype(np.float64)
    result = result[['lon', 'lat', 't', 'properties'] + added_columns]
    if input_duration_bool:
        result[input_duration_column] = result[input_duration_column].apply(
            lambda x: x.total_seconds() / time_conversions[input_duration_unit_time] if pd.notna(x) else 0
        )

    result['properties'] = result.apply(merge_json, axis=1)
    result = result[['lon', 'lat', 't', 'properties']]

    return result.to_dict(orient='records')
$$;
