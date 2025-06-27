CREATE OR REPLACE FUNCTION @@workflows_temp@@.TRAJECTORY_OUTLIER_CLEANER(
    traj_id STRING,
    trajectory VARIANT,
    speed_threshold FLOAT,
    input_unit_distance STRING,
    input_unit_time STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('numpy','pandas','geopandas','movingpandas','shapely')
HANDLER = 'main'
AS $$
from datetime import timedelta
import warnings

import numpy as np
import pandas as pd
import geopandas as gpd
import movingpandas as mpd

def main(
  traj_id,
  trajectory,
  speed_threshold,
  input_unit_distance,
  input_unit_time
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
        "Hours": "h"
    }

    # Convert English names to short names
    distance_unit = distance_units[input_unit_distance]
    time_unit = time_units[input_unit_time]

    # build the DataFrame
    df = pd.DataFrame.from_records(trajectory)
    
    # If trajectory is empty, return empty list
    if df.empty:
        return []
    
    df['t'] = pd.to_datetime(df['t'])

    if df.t.nunique() <= 1:
        # Return the original trajectory
        df['logs'] = 'A valid trajectory should have at least two points'
        return df.to_dict(orient='records')

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

    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter('always')

        result = mpd.OutlierCleaner(traj).clean(v_max=speed_threshold, units=(distance_unit, time_unit))

        result = result.to_point_gdf().reset_index()
        result['lon'] = result.geometry.x.astype(np.float64)
        result['lat'] = result.geometry.y.astype(np.float64)
        result['t'] = result['t'].dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
        result = result.drop(columns=['traj_id', 'geometry'])

        if caught_warnings:
          result['logs'] = str(caught_warnings[0].message)
        else:
          result['logs'] = ''

        return result.to_dict(orient='records')
$$;
