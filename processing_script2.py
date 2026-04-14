import re
import pandas as pd
from prophet import Prophet
import boto3
import os

BUCKET = os.environ.get("BUCKET", "msc-thesis-snehal")
NEW_FILE = os.environ.get("NEW_FILE")
MASTER_FILE = "Output/demand_forecast_results.csv"

s3 = boto3.client("s3")


def load_csv(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"], encoding="latin-1")


def save_csv(df, bucket, key):
    s3.put_object(Bucket=bucket, Key=key, Body=df.to_csv(index=False).encode("utf-8"))


def clean_ledger(df):
    df.columns = df.columns.str.strip().str.replace('.', '', regex=False).str.replace(' ', '_')
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')

    numeric_cols = [
        'Qty_Received', 'Qty_Issues', 'Rate_received_at',
        'Rate_issued_at', 'Value_of_issue', 'Balance_Qty',
        'Balance_Value', 'Average_Rate'
    ]

    for col in numeric_cols:
        df[col] = df[col].astype(str).apply(lambda x: re.sub(r'[^0-9.\-]', '', x))
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df['Customer'] = df['Customer'].astype(str).str.strip()
    return df


def classify_movement(months_since_last_move, avg_monthly):
    if months_since_last_move > 24:
        return "Dead Stock (2+ years no movement)"
    elif months_since_last_move > 6:
        return "Slow Moving (6+ months no movement)"
    elif avg_monthly >= 50:
        return "Fast Moving"
    elif avg_monthly > 0:
        return "Slow Moving"
    return "Dead Stock"


def forecast_part(data_part):
    supplier_mask = data_part['Customer'].str.lower() == "fronius international gmbh"
    issued_mask = ~supplier_mask

    total_received = data_part.loc[supplier_mask, 'Qty_Received'].sum()
    total_issued = data_part.loc[issued_mask, 'Qty_Issues'].sum()
    mean_received_rate = data_part.loc[supplier_mask, 'Rate_received_at'].mean()
    mean_issued_rate = data_part.loc[issued_mask, 'Rate_issued_at'].mean()
    unique_customers = data_part.loc[issued_mask, 'Customer'].dropna().unique().tolist()

    latest_row = data_part.loc[data_part['Date'].notna()].sort_values('Date').iloc[-1]
    latest_balance = latest_row['Balance_Qty']
    latest_balance_value = latest_row['Balance_Value']

    monthly_demand = (
        data_part.loc[issued_mask]
        .groupby(pd.Grouper(key='Date', freq='M'))['Qty_Issues']
        .sum()
        .reset_index()
    )

    projected_3m_demand = 0
    gap = latest_balance
    movement_class = "Insufficient Data / No Movement"

    try:
        if monthly_demand['Qty_Issues'].sum() > 0 and len(monthly_demand) > 2:
            prophet_df = monthly_demand.rename(columns={'Date': 'ds', 'Qty_Issues': 'y'})
            model = Prophet()
            model.fit(prophet_df)

            future = model.make_future_dataframe(periods=3, freq='M')
            forecast = model.predict(future)
            projected_3m_demand = forecast[['ds', 'yhat']].tail(3)['yhat'].sum()
            gap = latest_balance - projected_3m_demand

            last_movement = data_part.loc[
                issued_mask & (data_part['Qty_Issues'] > 0), 'Date'
            ].max()

            months_since = (
                (data_part['Date'].max() - last_movement).days // 30
                if pd.notnull(last_movement) else 9999
            )

            movement_class = classify_movement(months_since, monthly_demand['Qty_Issues'].mean())

    except Exception as e:
        movement_class = f"Forecast Failed: {str(e)}"

    return {
        "Part_Number": data_part['Part_Number'].iloc[0],
        "Current_Balance": latest_balance,
        "Projected_3M_Demand": projected_3m_demand,
        "Inventory_Gap": gap,
        "Total_Received_Qty": total_received,
        "Total_Issued_Qty": total_issued,
        "Mean_Received_Rate": mean_received_rate,
        "Mean_Issued_Rate": mean_issued_rate,
        "Current_Inventory_Value": latest_balance_value,
        "Movement_Class": movement_class,
        "Customers": ", ".join(unique_customers) if unique_customers else "N/A"
    }


def main():
    new_data = clean_ledger(load_csv(BUCKET, NEW_FILE))

    try:
        master = load_csv(BUCKET, MASTER_FILE)
    except Exception:
        master = pd.DataFrame()

    active_parts = new_data['Part_Number'].unique()
    print(f"Processing {len(active_parts)} active parts")

    new_results = [
        forecast_part(new_data[new_data['Part_Number'] == part].copy())
        for part in active_parts
    ]

    new_df = pd.DataFrame(new_results)

    if not master.empty:
        master = master[~master['Part_Number'].isin(active_parts)]
        updated = pd.concat([master, new_df], ignore_index=True)
    else:
        updated = new_df

    save_csv(updated, BUCKET, MASTER_FILE)
    print(f"Done. Results saved to s3://{BUCKET}/{MASTER_FILE}")


if __name__ == "__main__":
    main()