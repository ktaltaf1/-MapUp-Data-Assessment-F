import pandas as pd


def calculate_distance_matrix(df)->pd.DataFrame():
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    # Write your logic here
    # Create an empty DataFrame with unique IDs as both index and columns
    unique_ids = df['ID'].unique()
    distance_matrix = pd.DataFrame(index=unique_ids, columns=unique_ids)

    # Initialize the distance matrix with zeros
    distance_matrix = distance_matrix.fillna(0)

    # Iterate over the rows of the input DataFrame and populate the distance matrix
    for index, row in df.iterrows():
        source = row['Source']
        destination = row['Destination']
        distance = row['Distance']

        # Update the distance matrix with the cumulative distances
        distance_matrix.loc[source, destination] += distance
        distance_matrix.loc[destination, source] += distance  # Accounting for bidirectional distances

    # Update the original DataFrame with the cumulative distances
    for index, row in df.iterrows():
        source = row['Source']
        destination = row['Destination']
        distance = distance_matrix.loc[source, destination]

        df.at[index, 'Distance'] = distance

    return df


def unroll_distance_matrix(df)->pd.DataFrame():
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    # Write your logic here
    # Initialize an empty list to store unrolled data
    unrolled_data = []

    # Iterate over the rows and columns of the distance matrix
    for id_start in df.index:
        for id_end in df.columns:
            # Exclude same id_start to id_end combinations
            if id_start != id_end:
                distance = df.loc[id_start, id_end]
                unrolled_data.append({'id_start': id_start, 'id_end': id_end, 'distance': distance})

    # Create a DataFrame from the unrolled data
    unrolled_df = pd.DataFrame(unrolled_data)

    # Update the original DataFrame with the unrolled data
    df = unrolled_df.copy()

    return df


def find_ids_within_ten_percentage_threshold(df, reference_id)->pd.DataFrame():
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    # Write your logic here
    # Filter DataFrame based on the reference_id
    reference_data = df[df['id_start'] == reference_id]

    # Calculate the average distance for the reference_id
    reference_avg_distance = reference_data['distance'].mean()

    # Calculate the percentage threshold
    threshold = 0.1  # 10%

    # Calculate the lower and upper bounds for the threshold
    lower_bound = reference_avg_distance - (reference_avg_distance * threshold)
    upper_bound = reference_avg_distance + (reference_avg_distance * threshold)

    # Filter DataFrame based on the average distance within the specified threshold
    filtered_df = df.groupby('id_start')['distance'].mean().reset_index()
    filtered_df = filtered_df[(filtered_df['distance'] >= lower_bound) & (filtered_df['distance'] <= upper_bound)]

    # Sort the DataFrame by average distance
    filtered_df = filtered_df.sort_values(by='distance')

    # Update the original DataFrame with the filtered data
    df = df[df['id_start'].isin(filtered_df['id_start'])]

    return df


def calculate_toll_rate(df)->pd.DataFrame():
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Wrie your logic here
    # Define rate coefficients for each vehicle type
    rate_coefficients = {'moto': 0.8, 'car': 1.2, 'rv': 1.5, 'bus': 2.2, 'truck': 3.6}

    # Iterate over each vehicle type and calculate toll rates
    for vehicle_type, rate_coefficient in rate_coefficients.items():
        # Create a new column for the toll rates based on the distance and rate coefficient
        df[vehicle_type] = df['distance'] * rate_coefficient

    return df


def calculate_time_based_toll_rates(df)->pd.DataFrame():
    """
    Calculate time-based toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Write your logic here
    # Define time intervals and discount factors
    time_intervals = [
        {'start': datetime.time(0, 0, 0), 'end': datetime.time(10, 0, 0), 'weekday_factor': 0.8, 'weekend_factor': 0.7},
        {'start': datetime.time(10, 0, 0), 'end': datetime.time(18, 0, 0), 'weekday_factor': 1.2, 'weekend_factor': 0.7},
        {'start': datetime.time(18, 0, 0), 'end': datetime.time(23, 59, 59), 'weekday_factor': 0.8, 'weekend_factor': 0.7},
    ]

    # Create new columns for start_day, start_time, end_day, and end_time
    df['start_day'] = df['end_day'] = df['start_time'] = df['end_time'] = ""

    # Iterate over each time interval and update the DataFrame
    for interval in time_intervals:
        weekday_filter = (df['start_time'].dt.time >= interval['start']) & (df['start_time'].dt.time <= interval['end']) & (df['start_day'].isin(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']))
        weekend_filter = (df['start_time'].dt.time >= interval['start']) & (df['start_time'].dt.time <= interval['end']) & (df['start_day'].isin(['Saturday', 'Sunday']))

        # Update the DataFrame based on the time interval and day type
        df.loc[weekday_filter, ['start_day', 'start_time', 'end_day', 'end_time']] = ["Monday", interval['start'], "Monday", interval['end']]
        df.loc[weekend_filter, ['start_day', 'start_time', 'end_day', 'end_time']] = ["Saturday", interval['start'], "Saturday", interval['end']]

        # Apply discount factors to vehicle columns based on the time interval and day type
        for vehicle_type in ['moto', 'car', 'rv', 'bus', 'truck']:
            df.loc[weekday_filter, vehicle_type] *= interval['weekday_factor']
            df.loc[weekend_filter, vehicle_type] *= interval['weekend_factor']

    return df
