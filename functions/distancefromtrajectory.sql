CREATE OR REPLACE FUNCTION @@workflows_temp@@.DISTANCE_FROM_TRAJECTORY(
    traj_id STRING,
    trajectory VARIANT,
    position STRING,
    distance_from STRING,
    units STRING
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('numpy','pandas','geopandas','movingpandas','shapely','pyproj')
HANDLER = 'main'
AS $$
import numpy as np
import pandas as pd
import geopandas as gpd
import movingpandas as mpd
import shapely
import math

from pyproj import Transformer
from movingpandas.unit_utils import get_conversion

def main(
    traj_id,
    trajectory,
    position,
    distance_from,
    units,
):
    # Unit mapping from English names to short names
    distance_units = {
        "Kilometers": "km",
        "Meters": "m",
        "Miles": "mi",
        "Nautical Miles": "nm"
    }

    # Convert English names to short names
    unit = distance_units[units]

    if not trajectory:
        return None

    # Load the position as a geometry in World Mercator
    position = shapely.wkt.loads(position)
    latitude_degrees = position.y  # Get latitude before transformation
    transformer = Transformer.from_crs('EPSG:4326', 'EPSG:3395', always_xy=True)
    position = shapely.Point(*transformer.transform(position.x, position.y))

    # Compute the correction factor
    latitude_radians = math.radians(latitude_degrees)
    correction_factor = math.cos(latitude_radians)

    # build the DataFrame
    df = pd.DataFrame.from_records(trajectory)

    # Handle empty trajectory case
    if df.empty:
        return None

    # CRITICAL: Convert timestamps to datetime (Snowflake compatibility)
    df['t'] = pd.to_datetime(df['t'])

    # build the GeoDataFrame
    gdf = (
        gpd.GeoDataFrame(
            df[['t', 'properties']],
            geometry=gpd.points_from_xy(df.lon, df.lat),
            crs=4326
        )
        .set_index('t')
        .to_crs('EPSG:3395')  # Reproject to Web Mercator
    )

    if df.t.nunique() <= 1 or distance_from == 'First Point':
        distance = gdf.iloc[0].geometry.distance(position)
        conversion = get_conversion(unit, 'm')
        return distance * correction_factor / conversion.distance
    elif distance_from == 'Last Point':
        distance = gdf.iloc[-1].geometry.distance(position)
        conversion = get_conversion(unit, 'm')
        return distance * correction_factor / conversion.distance
    elif distance_from == 'Nearest Point':
        traj = mpd.Trajectory(gdf, traj_id)
        return traj.distance(other=position, units=unit) * correction_factor
$$;
