CREATE OR REPLACE FUNCTION @@workflows_temp@@.GET_VALUES_AT_TIMESTAMP(
    traj_id STRING,
    trajectory VARIANT,
    timestamp_str STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('numpy','pandas','geopandas>=1.0.0','movingpandas==0.22.3','shapely')
HANDLER = 'main'
AS $$
import numpy as np
import pandas as pd
import geopandas as gpd
import movingpandas as mpd
from datetime import datetime, timezone
from shapely.wkt import dumps

def main(
    traj_id,
    trajectory,
    timestamp_str
):

    t_at = datetime.fromisoformat(timestamp_str.replace('Z', '')).replace(tzinfo=None)

    # build the DataFrame
    df = pd.DataFrame.from_records(trajectory)
    df['t'] = pd.to_datetime(df['t'])  # MANDATORY for Snowflake timestamp conversion

    # Check if trajectory has enough unique timestamps
    if df.empty or df.t.nunique() <= 1:
        return None

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

     # Get the nearest positions to the provided timestamp
    geoms = traj.interpolate_position_at(t_at)

    return {
        "t": t_at,
        "geom": dumps(geoms)
    } if geoms else None
$$;