#!/usr/bin/env python
# coding: utf-8

# # Import Statements

from sys import version_info
import pandas as pd
import glob
import os

import matplotlib.pyplot as plt
import seaborn as sns
import graphviz

import dask
from dask.distributed import Client, LocalCluster

import dask.dataframe as dd
import dask.array as da

from dask_ml.preprocessing import OneHotEncoder
from dask_ml.preprocessing import StandardScaler
from dask_ml.compose import ColumnTransformer

os.chdir('/home/ubuntu/Scaleable-Ml/data')

# # Read & Combine Data

@dask.delayed
def load_origin():
    origin_filenames = [i for i in glob.glob('historical_data_20*.{}'.format('txt'))]
    origin_filenames.sort()
    # origin_filenames= origin_filenames[:1]

    origin_col_names = ['credit_score', 'first_payment_date', 'first_time_buyer', 'maturity_date', 'msa_code',
                        'mi_percent', 'unit_ct', 'occupancy_status', 'comb_loan_to_value', 'debt_to_income',
                        'org_upb', 'loan_to_value', 'org_roi', 'channel', 'ppm',
                        'rate_type', 'state', 'prop_type', 'pincode', 'seq_num',
                        'loan_purpose', 'org_term', 'num_borrowers', 'seller_name', 'servicer_name',
                        'sup_conforming', 'pre_harp_seq_num', 'program_indicator', 'harp_indicator', 'valuation_method',
                        'io_indicator']

    combined_origin = dd.concat([dd.read_csv(f, sep='|', engine='python', header=None, names=origin_col_names,
                                             dtype={
                                                 'sup_conforming': 'object',
                                                 'mi_percent': 'object',
                                                 'seq_num': 'object',
                                                 'pre_harp_seq_num': 'object', 
                                                 'harp_indicator': 'object' 
                                             }) for f in origin_filenames])

    return combined_origin


@dask.delayed
def load_perf():
    perf_filenames = [i for i in glob.glob('historical_data_time_*.{}'.format('txt'))]
    perf_filenames.sort()
    # perf_filenames = perf_filenames[:1]

    perf_col_names = ['seq_num', 'reporting_period', 'cur_upb', 'delinquency_status', 'loan_age',
                      'months_to_maturity', 'repurchased', 'modified', 'zero_bal_code', 'zero_bal_date',
                      'cur_roi', 'cur_def_upb', 'last_due_date', 'mi_recovery', 'net_sales_profit',
                      'non_mi_recovery', 'expenses', 'legal_cost', 'maintenance_cost', 'tax_insurance',
                      'misc_expenses', 'act_loss', 'modification_cost', 'step_modification', 'def_payment_plan',
                      'est_loan_to_value', 'zero_bal_removal_upb', 'delinquent_interest', 'delinquency_due_disaster',
                      'borrower_assistance_status']

    combined_perf = dd.concat([dd.read_csv(f, sep='|', engine='python', header=None, names=perf_col_names,
                                           dtype={
                                               'delinquency_status': 'object',
                                               'modified': 'object',
                                               'seq_num': 'object',
                                               'step_modification': 'object',
                                               'def_payment_plan': 'object'
                                           }) for f in perf_filenames])

    return combined_perf


# # Pre-processing & Data Cleaning

# ### Clean Origination Data

@dask.delayed
def clean_origin(df):
    df['credit_score'] = df['credit_score'].map(lambda x: x if x > 300 and x < 851 else -1)
    df['mi_percent'] = df['mi_percent'].map(lambda x: -1 if x == 999 else x)
    df['unit_ct'] = df['unit_ct'].map(lambda x: -1 if x == 99 else x)
    df['occupancy_status'] = df['mi_percent'].map(lambda x: -1 if x == 999 else x)
    df['comb_loan_to_value'] = df['unit_ct'].map(lambda x: -1 if x == 999 else x)
    df['debt_to_income'] = df['debt_to_income'].map(lambda x: -1 if x == 999 else x)
    df['loan_to_value'] = df['loan_to_value'].map(lambda x: -1 if x == 999 else x)
    df['loan_purpose'] = df['loan_purpose'].map(lambda x: -1 if x == 9 else x)
    df['num_borrowers'] = df['num_borrowers'].map(lambda x: -1 if x == 99 else x)
    df['valuation_method'] = df['valuation_method'].map(lambda x: -1 if x == 9 else x)

    # low variance
    return df.drop(columns=['pre_harp_seq_num', 'harp_indicator', 'io_indicator', 'ppm'])


# ### Clean Performance Data

@dask.delayed
def clean_perf(df):
    df['repurchased'] = df['repurchased'].map(lambda x: '1' if x == 'Y' else '0')
    df['modified'] = df['modified'].map(lambda x: '1' if x == 'Y' else '0')
    df['net_sales_profit'] = df['net_sales_profit'].map(lambda x: -1 if x == 'U' else x)
    df['step_modification'] = df['step_modification'].map(lambda x: '1' if x == 'Y' else '0')
    df['def_payment_plan'] = df['def_payment_plan'].map(lambda x: '1' if x == 'Y' else '0')
    df['delinquency_due_disaster'] = df['delinquency_due_disaster'].map(lambda x: '1' if x == 'Y' else '0')

    return df


# ### Transform label to numerical to calculate pearson correlation with features

def id_delinquent(status, bal_code):
    if status in ['0', '1', '2', 'R']:
        if bal_code in [3, 6, 9]:
            return 1
        else:
            return 0
    else:
        return 1


def transform_label(df):
    return df.apply(lambda x: id_delinquent(x['delinquency_status'], x['zero_bal_code']), axis=1)


@dask.delayed
def binarize_label(df):
    df['delinquency_status'] = df.map_partitions(lambda part: transform_label(part))

    return df


# # Feature Engineering

# ### Aggregate Performance Features

@dask.delayed
def aggregate_features(df):
    df = df.groupby('seq_num').agg({
        'cur_upb' : 'mean',
        'cur_def_upb' : 'mean',
        'mi_recovery' : 'mean',
        'net_sales_profit' : 'mean',
        'non_mi_recovery' : 'mean',
        'expenses': 'mean',
        'legal_cost': 'mean',
        'maintenance_cost': 'mean',
        'tax_insurance': 'mean',
        'misc_expenses': 'mean',
        'act_loss': 'mean',
        'modification_cost': 'mean',
        'zero_bal_removal_upb' : 'mean',
        'delinquent_interest' : 'mean',
        'months_to_maturity' : 'min',
        'loan_age': 'max',
        'delinquency_status' : 'max',
        'modified' : 'max',
        'zero_bal_code' : 'max',
        'step_modification' : 'max',
    }).reset_index()

    return df


# ### Join Origination & Performance Data for Feature Selection

@dask.delayed
def join_dfs(df1, df2, col):
    return df1.merge(df2, on=col).set_index(col)


# ### Standard Scale numerical features & One-Hot Encode categorical features

@dask.delayed
def engineer_features(df):
    df = df.categorize()
    cat_feat = df.select_dtypes(include=['object', 'category']).columns.tolist()
    num_feat = df.select_dtypes(exclude=['object', 'category']).columns.tolist()
    num_feat.remove('delinquency_status')

    label = df['delinquency_status'].copy()

    ct = ColumnTransformer([
        ("one-hot", OneHotEncoder(sparse=False), cat_feat),
        ("scale", StandardScaler(), num_feat)
    ])

    out = ct.fit_transform(df)
    out['delinquency_status'] = label

    return out


# # Feature Selection

# ### Construct Pearson Correlation Matrix

@dask.delayed
def calc_correlation(df):
    return df.corr()


# ### Plot correlation of features with target

def plot_correlation(df):
    fig, ax = plt.subplots(figsize=(10,30))
    heatmap = sns.heatmap(df[['delinquency_status']].sort_values('delinquency_status'),
                          vmax=1, vmin=-1, cmap='YlGnBu', annot=True, ax=ax);
    heatmap.set_title('Correlation Heatmap', fontdict={'fontsize':12}, pad=12);
    ax.invert_yaxis()

    return fig


# ### Select features based on correlation

@dask.delayed
def select_features(df, df_corr):
    vals = df_corr[['delinquency_status']].sort_values('delinquency_status')['delinquency_status'].to_dict()
    features = []

    for key, val in zip(vals.keys(), vals.values()):
        if abs(val) >= 0.1 and key != 'delinquency_status':
            features.append(key)

    return df[features]


# # Dask Visualize Task Graph & Computation

if __name__ == '__main__':
    try:
        cluster = LocalCluster(dashboard_address=':8787')
        client = Client(cluster)

        origin = load_origin()
        origin = clean_origin(origin)

        perf = load_perf()
        perf = clean_perf(perf)
        perf = binarize_label(perf)
        perf = aggregate_features(perf)

        df = join_dfs(origin, perf, 'seq_num')
        df = engineer_features(df).compute()

        df_corr = calc_correlation(df).compute()
        fig = plot_correlation(df_corr)

        df = select_features(df, df_corr).compute()
        # Output as parquet file

        print(df.columns)
        df.to_parquet('features.parquet')
    except TimeoutError:
        print('not working')
