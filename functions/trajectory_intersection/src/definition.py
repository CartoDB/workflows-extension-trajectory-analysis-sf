# /// script
# requires-python = "==3.11"
# dependencies = [
#   "numpy",
#   "pandas",
#   "geopandas==1.1.1",
#   "movingpandas==0.22.3",
#   "shapely",
# ]
# ///

import numpy as np
import pandas as pd
import geopandas as gpd
import movingpandas as mpd
import json
import shapely
from shapely.wkt import loads

def main(
  traj_id,
  trajectory,
  polygon,
  intersection_method
):
    if not trajectory:
        return trajectory

    point_based = intersection_method == 'Points'
    polygon = loads(polygon)

    # build the DataFrame
    df = pd.DataFrame.from_records(trajectory)

    # If trajectory is empty, return empty list
    if df.empty:
        return []

    # MANDATORY timestamp conversion for Snowflake - ensure UTC timezone
    df['t'] = pd.to_datetime(df['t'])
    if df['t'].dt.tz is None:
        df['t'] = df['t'].dt.tz_localize('UTC')
    else:
        df['t'] = df['t'].dt.tz_convert('UTC')

    # build the GeoDataFrame
    gdf = (
      gpd.GeoDataFrame(
        df[['t', 'properties']],
        geometry=gpd.points_from_xy(df.lon, df.lat),
        crs=4326
      )
      .set_index('t')
    )

    if df.empty or df.t.nunique() <= 1:
        if shapely.intersects(gdf.geometry.iloc[0], polygon):
            return trajectory
        else:
            return []

    # build the Trajectory object
    traj = mpd.Trajectory(gdf, traj_id)

    # For point-based intersection, check each point against polygon
    if point_based:
        intersecting_indices = gdf.intersects(polygon)
        if intersecting_indices.any():
            intersecting_gdf = gdf[intersecting_indices]
            result = []
            for idx, row in intersecting_gdf.iterrows():
                result.append({
                    "lon": row.geometry.x,
                    "lat": row.geometry.y,
                    "t": idx.strftime('%Y-%m-%d %H:%M:%S.%f+00:00'),
                    "properties": row['properties']
                })
            return result
        else:
            return []
    else:
        # For segment-based intersection, use MovingPandas clip functionality
        try:
            clipped = traj.clip(polygon)
            if clipped is None or clipped.is_empty:
                return []

            # Extract points from clipped trajectory
            result = []
            for idx, row in clipped.df.iterrows():
                result.append({
                    "lon": row.geometry.x,
                    "lat": row.geometry.y,
                    "t": idx.strftime('%Y-%m-%d %H:%M:%S.%f+00:00'),
                    "properties": row['properties']
                })
            return result
        except Exception:
            # Fallback to point-based intersection if clip fails
            intersecting_indices = gdf.intersects(polygon)
            if intersecting_indices.any():
                intersecting_gdf = gdf[intersecting_indices]
                result = []
                for idx, row in intersecting_gdf.iterrows():
                    result.append({
                        "lon": row.geometry.x,
                        "lat": row.geometry.y,
                        "t": idx.strftime('%Y-%m-%d %H:%M:%S.%f+00:00'),
                        "properties": row['properties']
                    })
                return result
            else:
                return []