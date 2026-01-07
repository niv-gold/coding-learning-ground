import pandas as pd
import numpy as np

def solution(A, D):
    # Ensure A and D have the same length
    if len(A) != len(D):
        raise ValueError("A and D must have the same length")
    # Create DataFrame directly from A and D
    df_trans = pd.DataFrame({'trans_amount': A, 'trans_date': D})    
    # Convert 'trans_date' to datetime
    df_trans['trans_date'] = pd.to_datetime(df_trans['trans_date'], format='%Y-%m-%d')    
    # Sort the DataFrame by date
    df_trans = df_trans.sort_values('trans_date')    
    # Calculate running total
    df_trans['running_total'] = df_trans['trans_amount'].cumsum()    
    # Calculate monthly running total
    df_trans['month'] = df_trans['trans_date'].dt.to_period('M')
    df_trans['Monthly_Running_Total'] = df_trans.groupby('month')['trans_amount'].cumsum()
    # Count negative transactions per month
    df_trans['is_negative'] = df_trans['trans_amount'] < 0
    df_trans['Negative_Trans_Count'] = df_trans.groupby('month')['is_negative'].cumsum()
     # Calculate sum of negative transactions
    df_trans['negative_amount'] = df_trans['trans_amount'].where(df_trans['is_negative'], 0)
    df_trans['Negative_Trans_Sum'] = df_trans.groupby('month')['negative_amount'].cumsum()
    # Add column with last negative sum for each month
    df_trans['Month_End_Negative_Sum'] = df_trans.groupby('month')['Negative_Trans_Sum'].transform('last')
    # Add column that shows -5 for the last row of each month if Month_End_Negative_Sum <= -100
    df_trans['is_last_in_month'] = df_trans.groupby('month')['trans_date'].transform('max') == df_trans['trans_date']
    df_trans['Month_End_Adjustment'] = np.where(
        (df_trans['is_last_in_month']) & (df_trans['Month_End_Negative_Sum'] <= -100),
        -5,
        0
    )
     # Calculate the running total including Month_End_Adjustment
    df_trans['Adjusted_Amount'] = df_trans['trans_amount'] + df_trans['Month_End_Adjustment']
    df_trans['Cumulative_Adjusted_Total'] = df_trans['Adjusted_Amount'].cumsum()
    # Get the final value from the last day
    final_value = df_trans['Cumulative_Adjusted_Total'].iloc[-1]
    return final_value


# Test the function
A = [180, -50, -25, -25,
     -15 , -28 , -5, 500,
     -256, 25, 66, 48,
     88, 700
     ]
D = ['2020-01-01', '2020-01-01', '2020-01-31', '2020-01-29', 
     '2020-02-01', '2020-02-01', '2020-02-03', '2020-02-01',
     '2020-03-01', '2020-03-01', '2020-03-15', '2020-03-14',
     '2020-04-01', '2020-04-01',     
     ]

solution(A, D)


def ddd