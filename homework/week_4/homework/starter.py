import sys
import pickle
import pandas as pd

year = int(sys.argv[1])
month = int(sys.argv[2])
output_file = f'fhv_tripdata_{year:04d}_{month:02d}.parquet'
input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04}-{month:02}.parquet'

with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)

categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

df = read_data(input_file)

dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)

df_preds = pd.DataFrame(y_pred, columns=['Predictions'])
df_preds['ride_id'] = f'{year:04}_{month:02}_' + df_preds.index.astype('str')

print(df_preds['Predictions'].mean())

df_preds.to_parquet(path=f'data/{output_file}', engine='pyarrow', compression=None, index=False)
