import pandas as pd
import pyarrow.parquet as pq
import s3fs
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

df_features = pd.read_parquet('s3://ds102-team-x-scratch/outputs/features_2019Q1.parquet', engine='auto')

s3 = s3fs.S3FileSystem()

label_df = pq.ParquetDataset('s3://ds102-team-x-scratch/outputs/labels_2019Q1.parquet', filesystem=s3) \
    .read_pandas().to_pandas()

df = df_features.merge(label_df, left_on='seq_num', right_on='seq_num')

df['zero_bal_code'].fillna(value=df['zero_bal_code'].mean(), inplace=True)

data = df.to_numpy()

X = data[:, 1:-1]
y = data[:, -1]
y = y.astype('float')

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

clf = LogisticRegression(random_state=0).fit(X_train, y_train)
print("Test Accuracy: {}".format(clf.score(X_test, y_test)))

