-- TRAJECTORY_STOP_POINTS
CREATE OR REPLACE FUNCTION @@workflows_temp@@.TRAJECTORY_STOP_POINTS(
    traj_id STRING,
    trajectory ARRAY,
    max_diameter FLOAT,
    min_duration FLOAT,
    duration_unit STRING
)
RETURNS ARRAY
LANGUAGE python
RUNTIME_VERSION = '3.11'
PACKAGES = ('numpy','pandas','geopandas>=1.0.0','movingpandas==0.22.3','shapely')
HANDLER = 'main'
AS
$$
from datetime import timedelta

import numpy as np
import pandas as pd
import geopandas as gpd
import movingpandas as mpd

def main(
    traj_id,
    trajectory,
    max_diameter,
    min_duration,
    duration_unit,
):
    # Unit mapping from English names to short names
    time_units = {
        "Seconds": "seconds",
        "Minutes": "minutes",
        "Hours": "hours",
        "Days": "days"
    }

    # build the DataFrame
    df = pd.DataFrame.from_records(trajectory)

    if df.empty or df.t.nunique() <= 1:
        # Return no known stops
        return []

    # Convert timestamp column to datetime
    df['t'] = pd.to_datetime(df['t'])

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

    # Convert duration to timedelta
    kwargs = {time_units[duration_unit]: min_duration}
    duration_td = timedelta(**kwargs)

    result = (
        mpd.TrajectoryStopDetector(traj)
        .get_stop_points(
            max_diameter=max_diameter,
            min_duration=duration_td,
        )
    )

    result = result.reset_index()
    result['geometry'] = result.geometry.to_wkt()
    
    # Convert timestamps to strings with timezone info to match expected format
    if 'start_time' in result.columns:
        result['start_time'] = result['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
        result['end_time'] = result['end_time'].dt.strftime('%Y-%m-%d %H:%M:%S+00:00')

    return result.to_dict(orient='records')
$$;

-- TRAJECTORY_STOP_SEGMENTS
CREATE OR REPLACE FUNCTION @@workflows_temp@@.TRAJECTORY_STOP_SEGMENTS(
    traj_id STRING,
    trajectory ARRAY,
    max_diameter FLOAT,
    min_duration FLOAT,
    duration_unit STRING
)
RETURNS ARRAY
LANGUAGE python
RUNTIME_VERSION = '3.11'
PACKAGES = ('numpy','pandas','geopandas>=1.0.0','movingpandas==0.22.3','shapely')
HANDLER = 'main'
AS
$$
from datetime import timedelta

import numpy as np
import pandas as pd
import geopandas as gpd
import movingpandas as mpd

def main(
    traj_id,
    trajectory,
    max_diameter,
    min_duration,
    duration_unit,
):
    # Unit mapping from English names to short names
    time_units = {
        "Seconds": "seconds",
        "Minutes": "minutes",
        "Hours": "hours",
        "Days": "days"
    }

    # build the DataFrame
    df = pd.DataFrame.from_records(trajectory)

    if df.shape[0] <= 1:
        # Return no known stops
        return []

    # Convert timestamp column to datetime
    df['t'] = pd.to_datetime(df['t'])

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

    # Convert duration to timedelta
    kwargs = {time_units[duration_unit]: min_duration}
    duration_td = timedelta(**kwargs)

    result = (
        mpd.TrajectoryStopDetector(traj)
        .get_stop_segments(
            max_diameter=max_diameter,
            min_duration=duration_td,
        )
    )

    if not result:
        return []

    result = result.to_point_gdf().reset_index()
    result['lon'] = result.geometry.x.astype(np.float64)
    result['lat'] = result.geometry.y.astype(np.float64)
    result['stop_id'] = result.traj_id
    result = result.drop(columns=['traj_id', 'geometry'])

    return result.to_dict(orient='records')
$$;