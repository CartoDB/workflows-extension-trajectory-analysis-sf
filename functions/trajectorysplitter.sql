-- TRAJECTORY_STOP_SPLITTER
CREATE OR REPLACE FUNCTION @@workflows_temp@@.TRAJECTORY_STOP_SPLITTER(
    traj_id STRING,
    trajectory ARRAY,
    min_duration FLOAT,
    duration_unit STRING,
    max_diameter FLOAT,
    min_length FLOAT
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

def main(traj_id, trajectory, min_duration, duration_unit, max_diameter, min_length):
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
        # Return the original trajectory
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')

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

    kwargs = {time_units[duration_unit]: min_duration}
    duration_td = timedelta(**kwargs)

    result = mpd.StopSplitter(traj).split(
        max_diameter=max_diameter,
        min_duration=duration_td,
        min_length=min_length
    )

    if len(result) == 0:
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')
    else:
        result = result.to_point_gdf().reset_index()
        result['lon'] = result.geometry.x.astype(np.float64)
        result['lat'] = result.geometry.y.astype(np.float64)
        result['seg_id'] = result.traj_id
        result = result.drop(columns=['traj_id', 'geometry'])
        return result.to_dict(orient='records')
$$;

-- TRAJECTORY_TEMPORAL_SPLITTER
CREATE OR REPLACE FUNCTION @@workflows_temp@@.TRAJECTORY_TEMPORAL_SPLITTER(
    traj_id STRING,
    trajectory ARRAY,
    mode STRING,
    min_length FLOAT
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

def main(traj_id, trajectory, mode, min_length):
    # build the DataFrame
    df = pd.DataFrame.from_records(trajectory)

    if df.empty or df.t.nunique() <= 1:
        # Return the original trajectory
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')

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

    result = mpd.TemporalSplitter(traj).split(
        mode=mode.lower(),
        min_length=min_length
    )

    if len(result) == 0:
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')
    else:
        result = result.to_point_gdf().reset_index()
        result['lon'] = result.geometry.x.astype(np.float64)
        result['lat'] = result.geometry.y.astype(np.float64)
        result['seg_id'] = result.traj_id
        result = result.drop(columns=['traj_id', 'geometry'])
        return result.to_dict(orient='records')
$$;

-- TRAJECTORY_SPEED_SPLITTER
CREATE OR REPLACE FUNCTION @@workflows_temp@@.TRAJECTORY_SPEED_SPLITTER(
    traj_id STRING,
    trajectory ARRAY,
    min_speed FLOAT,
    min_duration FLOAT,
    duration_unit STRING,
    min_length FLOAT
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
import json

def main(traj_id, trajectory, min_speed, min_duration, duration_unit, min_length):
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
        # Return the original trajectory
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')

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

    kwargs = {time_units[duration_unit]: min_duration}
    duration_td = timedelta(**kwargs)

    result = mpd.SpeedSplitter(traj).split(
        speed=min_speed,
        duration=duration_td,
        min_length=min_length
    )

    if len(result) == 0:
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')
    else:
        result = result.to_point_gdf().reset_index()
        result['lon'] = result.geometry.x.astype(np.float64)
        result['lat'] = result.geometry.y.astype(np.float64)
        result['seg_id'] = result.traj_id
        result = result.drop(columns=['traj_id', 'geometry','speed'])
        return result.to_dict(orient='records')
$$;

-- TRAJECTORY_OBSERVATION_SPLITTER
CREATE OR REPLACE FUNCTION @@workflows_temp@@.TRAJECTORY_OBSERVATION_SPLITTER(
    traj_id STRING,
    trajectory ARRAY,
    min_duration FLOAT,
    duration_unit STRING,
    min_length FLOAT
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

def main(traj_id, trajectory, min_duration, duration_unit, min_length):
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
        # Return the original trajectory
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')

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

    kwargs = {time_units[duration_unit]: min_duration}
    duration_td = timedelta(**kwargs)

    result = mpd.ObservationGapSplitter(traj).split(
        gap=duration_td,
        min_length=min_length
    )

    if len(result) == 0:
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')
    else:
        result = result.to_point_gdf().reset_index()
        result['lon'] = result.geometry.x.astype(np.float64)
        result['lat'] = result.geometry.y.astype(np.float64)
        result['seg_id'] = result.traj_id
        result = result.drop(columns=['traj_id', 'geometry'])
        return result.to_dict(orient='records')
$$;

-- TRAJECTORY_VALUECHANGE_SPLITTER
CREATE OR REPLACE FUNCTION @@workflows_temp@@.TRAJECTORY_VALUECHANGE_SPLITTER(
    traj_id STRING,
    trajectory ARRAY,
    valuechange_col STRING,
    min_length FLOAT
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
import json

def main(traj_id, trajectory, valuechange_col, min_length):
    # build the DataFrame
    df = pd.DataFrame.from_records(trajectory)
    df['properties_dict'] = df['properties'].apply(json.loads)
    df[valuechange_col] = df['properties_dict'].apply(lambda x: x.get(valuechange_col))

    # check that valuechange_col is in properties
    if len(df.dropna()) == 0:
        raise ValueError(f'Specified value-change column {valuechange_col} not found in properties')

    if df.empty or df.t.nunique() <= 1:
        # Return the original trajectory
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')

    # Convert timestamp column to datetime
    df['t'] = pd.to_datetime(df['t'])

    # build the GeoDataFrame
    gdf = (
      gpd.GeoDataFrame(
        df[['t', 'properties', valuechange_col]],
        geometry=gpd.points_from_xy(df.lon, df.lat),
        crs=4326
      )
      .set_index('t')
    )

    # build the Trajectory object
    traj = mpd.Trajectory(gdf, traj_id)

    result = mpd.ValueChangeSplitter(traj).split(
        col_name=valuechange_col,
        min_length=min_length
    )

    if len(result) == 0:
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')
    else:
        result = result.to_point_gdf().reset_index()
        result['lon'] = result.geometry.x.astype(np.float64)
        result['lat'] = result.geometry.y.astype(np.float64)
        result['seg_id'] = result.traj_id
        result = result.drop(columns=['traj_id', 'geometry'])
        return result.to_dict(orient='records')
$$;

-- TRAJECTORY_ANGLECHANGE_SPLITTER
CREATE OR REPLACE FUNCTION @@workflows_temp@@.TRAJECTORY_ANGLECHANGE_SPLITTER(
    traj_id STRING,
    trajectory ARRAY,
    min_angle FLOAT,
    min_speed FLOAT,
    min_length FLOAT
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
import json

def main(traj_id, trajectory, min_angle, min_speed, min_length):
    # build the DataFrame
    df = pd.DataFrame.from_records(trajectory)
    if df.empty or df.t.nunique() <= 1:
        # Return the original trajectory
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')

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

    result = mpd.AngleChangeSplitter(traj).split(
        min_angle=min_angle,
        min_speed=min_speed,
        min_length=min_length
    )

    if len(result) == 0:
        df['seg_id'] = traj_id
        return df.to_dict(orient='records')
    else:
        result = result.to_point_gdf().reset_index()
        result['lon'] = result.geometry.x.astype(np.float64)
        result['lat'] = result.geometry.y.astype(np.float64)
        result['seg_id'] = result.traj_id
        result = result.drop(columns=['traj_id', 'geometry'])
        return result.to_dict(orient='records')
$$;