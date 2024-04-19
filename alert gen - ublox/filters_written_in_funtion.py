import numpy as np
import pandas as pd

def prepare_data(data):
    new_df = data.copy()

    # Round 'msl' column to 2 decimal places
    new_df['msl'] = np.round(new_df['msl'], 2)

    # Apply fix_type and sat_num filters
    new_df = new_df.loc[(new_df['fix_type'] == 2) & (new_df['sat_num'] > 28)]

    print('new_df fix=2, satnum>28:', len(new_df))
    return new_df

def apply_accuracy_filters(df, horizontal_accuracy, vertical_accuracy):
    # Apply horizontal and vertical accuracy filters
    df = df.loc[(df['hacc'] == horizontal_accuracy) & (df['vacc'] <= vertical_accuracy)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    print('new_df hacc vacc:', len(df))
    return df

def filter_decimal_precision(df):
    # Filter for complete decimal precision in latitude and longitude
    df = df[(df['latitude'].astype(str).str[-10] == '.') & (df['longitude'].astype(str).str[-10] == '.')]

    print('new_df complete deci:', len(df))
    return df

def filter_outliers_1(df):
    # Apply outlier filter based on latitude and longitude
    df = df.loc[~np.isnan(df['latitude'])].reset_index(drop=True)

    return df

def filter_outliers_2(df):
    # Apply outlier filter based on 'msl' (Mean Sea Level)
    df = df.copy()
    dfmean = df[['msl']].rolling(min_periods=1, window=24, center=False).mean()
    dfsd = df[['msl']].rolling(min_periods=1, window=24, center=False).std()

    dfulimits = dfmean + (2 * dfsd)
    dfllimits = dfmean - (2 * dfsd)

    df['msl'] = np.where((df['msl'] > dfulimits['msl']) | (df['msl'] < dfllimits['msl']), np.nan, df['msl'])
    df = df.dropna(subset=['msl'])

    return df

# Main data processing pipeline
def process_data(data):
    # Initial data preparation
    new_df = prepare_data(data)

    # Apply accuracy filters
    new_df = apply_accuracy_filters(new_df, horizontal_accuracy, vertical_accuracy)

    # Filter decimal precision
    new_df = filter_decimal_precision(new_df)

    # Apply outlier filters
    new_df = filter_outliers_1(new_df)
    new_df = filter_outliers_2(new_df)

    return new_df

# Call the data processing pipeline
processed_data = process_data(data)
print('Final length of processed data:', len(processed_data))
