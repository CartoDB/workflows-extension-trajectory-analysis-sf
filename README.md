# Trajectory Analysis Extension for Snowflake

This extension provides components for working with trajectory datasets inside CARTO Workflows. Users of this extension can clean and simplify trajectories, compute derivative metrics (speeds, positions, distances...), detect stops, and more.

The Trajectory Analysis extension is compatible with Workflows powered by Snowflake.

## Components

It includes 10 components:

- **Trajectory from Points**: Convert a set of points into a trajectory
- **Trajectory to Points**: Convert a trajectory into a set of points
- **Trajectory Splitter**: Split trajectories into segments based on time or distance
- **Detect Stops**: Identify stops within trajectories
- **Compute Trajectory Metrics**: Calculate speed, distance, and other trajectory metrics
- **Clean Trajectories**: Remove noise and outliers from trajectories
- **Trajectory Simplifier**: Reduce trajectory points while preserving shape
- **Get Position at Timestamp**: Extract interpolated trajectory positions at specific timestamps
- **Intersect Trajectories**: Find which trajectory segments intersect a polygon
- **Distance from Trajectory**: Calculate the minimum distance between points and trajectories

## Building the extension

To build the extension, follow these steps:

1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Package the extension:
   ```
   python carto_extension.py package
   ```

This will create a packaged version of the extension that can be installed in your CARTO Workflows.

## Running the tests

To run the tests follow [these instructions](https://github.com/CartoDB/workflows-extension-template/blob/master/doc/running_tests.md). You would also need to specify the Snowflake connection details in the `.env` file:

```
SF_USER="<your_user>"
SF_PASSWORD="<your_password>"
SF_ACCOUNT="<your_account>"
SF_TEST_DATABASE="<your_database>"
SF_TEST_SCHEMA="<your_schema>"
```
