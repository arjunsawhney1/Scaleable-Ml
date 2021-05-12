import dask.dataframe as dd

if __name__ == '__main__':
    origin_col_names = ['credit_score', 'first_payment_date', 'first_time_buyer', 'maturity_date', 'msa_code',
                        'mi_percent', 'unit_ct', 'occupancy_status', 'comb_loan_to_value', 'debt_to_income',
                        'org_upb', 'loan_to_value', 'org_roi', 'channel', 'ppm',
                        'rate_type', 'state', 'prop_type', 'pincode', 'seq_num',
                        'loan_purpose', 'org_term', 'num_borrowers', 'seller_name', 'servicer_name',
                        'sup_conforming', 'pre_harp_seq_num', 'program_indicator', 'harp_indicator', 'valuation_method',
                        'io_indicator']

    perf_col_names = ['seq_num', 'reporting_period', 'cur_upb', 'delinquency_status', 'loan_age',
                      'months_to_maturity', 'repurchased', 'modified', 'zero_bal_code', 'zero_bal_date',
                      'cur_roi', 'cur_def_upb', 'last_due_date', 'mi_recovery', 'net_sales_profit',
                      'non_mi_recovery', 'expenses', 'legal_cost', 'maintenance_cost', 'tax_insurance',
                      'misc_expenses', 'act_loss', 'modification_cost', 'step_modification', 'def_payment_plan',
                      'est_loan_to_value', 'zero_bal_removal_upb', 'delinquent_interest', 'delinquency_due_disaster',
                      'borrower_assistance_status']

    qtr = '2019Q1'
    df_origin = dd.read_csv('./data/historical_data_' + qtr + '/historical_data_' + qtr + '.txt', sep='|',
                     engine='python', header=None, names=origin_col_names)

    # print(df_origin.info())
    # print(len(df_origin['seq_num']))
    # print(df_origin['credit_score'].mean().compute())

    df_perf = dd.read_csv('./data/historical_data_' + qtr + '/historical_data_time_' + qtr + '.txt', sep='|',
                     engine='python', header=None, names=perf_col_names,
                     dtype={
                            'delinquency_status': 'object',
                            'modified': 'object',
                            'step_modification': 'object'
                     })

    deliq_map = {
        "0": 0,
        "1": 1,
        "2": 2,
        "3": 3,
        'R': 100,
        '': -1
    }
    df_perf['delinquency_status'] = df_perf['delinquency_status'].map(deliq_map)

    print(df_perf.groupby('delinquency_status').seq_num.count().compute())

    # print(df_perf.head())
