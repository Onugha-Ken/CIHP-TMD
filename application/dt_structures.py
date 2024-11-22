import vaex
import pandas as pd
import numpy as np

################################################################################################################
# Read datasets
################################################################################################################
# df_tll = pd.read_excel('Linelist_Merged.xlsx')
df_curr = pd.read_excel('fac_summary_curr.xlsx')
temp_table_curr = vaex.open('temp_table_curr.parquet')
temp_table_curr = temp_table_curr.to_pandas_df()
################################################################################################################
# Manipulate the summary and patient tables for TX_CURR
################################################################################################################
tx_curr_temp_tbl = df_curr.filter(['IP', 'State', 'SurgeCommand', 'LGA', 'DATIMCode', 'Facility Name', 'TX_CURR', 'MMD <3', 'MMD 3', 
                             'MMD 4-5', 'MMD 6','Biometrics Captured', 'Biometrics Coverage', 'Biometrics Recaptured', 'Recaptured Coverage'])

df_curr_list = temp_table_curr[temp_table_curr['CurrentARTStatus_28Days'] == 'Active']

df_pbs_list = temp_table_curr[temp_table_curr['Biometrics Captured'] == 'Yes']


################################################################################################################
# Manipulate the summary and patient tables for TX_NEW
################################################################################################################
tx_new_temp_tbl = df_curr.filter(['IP', 'State', 'SurgeCommand', 'LGA', 'DATIMCode', 'Facility Name', 'TX_NEW', 'TX_NEW month 1', 
                'TX_NEW month 2', 'TX_NEW month 3','TX_NEW Captured', '% TX_NEW Captured','TX_NEW Recaptured',  '% TX_NEW Recaptured',
                 'TX_NEW ML', 'TX_NEW IIT',  '% TX_NEW IIT','TX_NEW CD4','TX_NEW CD4 <200', 'TX_NEW CD4 ≥200', 'TX_NEW Unknown CD4'])

df_new_list = temp_table_curr[temp_table_curr['TX_NEW'] == 'Yes']

df_new_listmth1 = temp_table_curr[temp_table_curr['TX_NEW month 1'] == 'Yes']

df_new_listmth2 = temp_table_curr[temp_table_curr['TX_NEW month 2'] == 'Yes']

df_new_listmth3 = temp_table_curr[temp_table_curr['TX_NEW month 3'] == 'Yes']

df_pbs_new_list = temp_table_curr[temp_table_curr['TX_NEW Captured'] == 'Yes']

df_cd4_new_list = temp_table_curr[temp_table_curr['TX_NEW_CD4_2'] == 'Yes']

con2 = [((temp_table_curr['TX_NEW ML'] == 'Died') | (temp_table_curr['TX_NEW ML'] == 'IIT') | 
   (temp_table_curr['TX_NEW ML'] == 'Refused (stopped) treatment') | (temp_table_curr['TX_NEW ML'] == 'Transferred Out'))]
value2 = ['Yes']
temp_table_curr['TX_NEW ML_2'] = np.select(con2, value2, default='')

df_ml_new_list = temp_table_curr[temp_table_curr['TX_NEW ML_2'] == 'Yes']
################################################################################################################
# Manipulate the summary and patient tables for TX_ML 
################################################################################################################
tx_ml_df = df_curr.filter(['IP', 'State', 'SurgeCommand', 'LGA', 'DATIMCode', 'Facility Name', 'TX_CURR', 'TX_ML', 'Transferred Out', 
                           'Dead', 'Stopped Treatment', 'IIT', '% TX_CURR IIT', 'IIT <3Months_TX', 'IIT 3_5Mths_TX', 'IIT 6Months+_TX', 
                           'TX_ML Captured', 'TX_ML Recaptured'])

tx_ml_df_sorted = tx_ml_df.sort_values(by=['IIT'], ascending=False)

df_ml_list = temp_table_curr[temp_table_curr['TX_ML'] == 'Yes']
df_ml_2_list = temp_table_curr[temp_table_curr['TX_ML_Outcome_3'] == 'Yes']
df_ml_iit_list = temp_table_curr[temp_table_curr['IIT'] == 'Yes']
df_ml_iit_days_list = temp_table_curr[temp_table_curr['IIT_duration_days_2'] == 'Yes']

# Filter and group by 'IP' and 'mmd' for 'TX_CURR'
grouped_tx_curr = temp_table_curr.groupby(['IP', 'mmd'], as_index=False)['TX_CURR'].count()
# Filter and group by 'IP' and 'mmd' for 'IIT'
grouped_iit = temp_table_curr.groupby(['IP', 'mmd_iit'], as_index=False)['IIT'].count()
grouped_iit.rename(columns={'mmd_iit': 'mmd'}, inplace=True)
# Merge the grouped DataFrames on 'IP' and 'mmd'
tx_ml_df_ip_mmd = pd.merge(grouped_tx_curr, grouped_iit, on=['IP', 'mmd'], how='left')
tx_ml_df_ip_mmd['IIT Rate (%)'] = np.round((tx_ml_df_ip_mmd['IIT'] / tx_ml_df_ip_mmd['TX_CURR']) * 100, decimals=1)


# Filter and group by 'State' and 'mmd' for 'TX_CURR'
grouped_state_tx_curr = temp_table_curr.groupby(['State', 'mmd'], as_index=False)['TX_CURR'].count()
# Filter and group by 'State' and 'mmd' for 'IIT'
grouped_state_iit = temp_table_curr.groupby(['State', 'mmd_iit'], as_index=False)['IIT'].count()
grouped_state_iit.rename(columns={'mmd_iit': 'mmd'}, inplace=True)
# Merge the grouped DataFrames on 'State' and 'mmd'
tx_ml_df_state_mmd = pd.merge(grouped_state_tx_curr, grouped_state_iit, on=['State', 'mmd'], how='left')
tx_ml_df_state_mmd['IIT Rate (%)'] = np.round((tx_ml_df_state_mmd['IIT'] / tx_ml_df_state_mmd['TX_CURR']) * 100, decimals=1)

# Filter and group by 'LGA' and 'mmd' for 'TX_CURR'
grouped_lga_tx_curr = temp_table_curr.groupby(['LGA', 'mmd'], as_index=False)['TX_CURR'].count()
# Filter and group by 'LGA' and 'mmd' for 'IIT'
grouped_lga_iit = temp_table_curr.groupby(['LGA', 'mmd_iit'], as_index=False)['IIT'].count()
grouped_lga_iit.rename(columns={'mmd_iit': 'mmd'}, inplace=True)
# Merge the grouped DataFrames on 'LGA' and 'mmd'
tx_ml_df_lga_mmd = pd.merge(grouped_lga_tx_curr, grouped_lga_iit, on=['LGA', 'mmd'], how='left')
tx_ml_df_lga_mmd['IIT Rate (%)'] = np.round((tx_ml_df_lga_mmd['IIT'] / tx_ml_df_lga_mmd['TX_CURR']) * 100, decimals=1)

# Filter and group by 'SurgeCommand' and 'mmd' for 'TX_CURR'
grouped_comd_tx_curr = temp_table_curr.groupby(['SurgeCommand', 'mmd'], as_index=False)['TX_CURR'].count()
# Filter and group by 'SurgeCommand' and 'mmd' for 'IIT'
grouped_comd_iit = temp_table_curr.groupby(['SurgeCommand', 'mmd_iit'], as_index=False)['IIT'].count()
grouped_comd_iit.rename(columns={'mmd_iit': 'mmd'}, inplace=True)
# Merge the grouped DataFrames on 'SurgeCommand' and 'mmd'
tx_ml_df_comd_mmd = pd.merge(grouped_comd_tx_curr, grouped_comd_iit, on=['SurgeCommand', 'mmd'], how='left')
tx_ml_df_comd_mmd['IIT Rate (%)'] = np.round((tx_ml_df_comd_mmd['IIT'] / tx_ml_df_comd_mmd['TX_CURR']) * 100, decimals=1)

# Filter and group by 'FacilityName' and 'mmd' for 'TX_CURR'
grouped_fac_tx_curr = temp_table_curr.groupby(['FacilityName', 'mmd'], as_index=False)['TX_CURR'].count()
# Filter and group by 'FacilityName' and 'mmd' for 'IIT'
grouped_fac_iit = temp_table_curr.groupby(['FacilityName', 'mmd_iit'], as_index=False)['IIT'].count()
grouped_fac_iit.rename(columns={'mmd_iit': 'mmd'}, inplace=True)
# Merge the grouped DataFrames on 'FacilityName' and 'mmd'
tx_ml_df_fac_mmd = pd.merge(grouped_fac_tx_curr, grouped_fac_iit, on=['FacilityName', 'mmd'], how='left')
tx_ml_df_fac_mmd['IIT Rate (%)'] = np.round((tx_ml_df_fac_mmd['IIT'] / tx_ml_df_fac_mmd['TX_CURR']) * 100, decimals=1)
################################################################################################################
# Manipulate the summary and patient tables for TX_RTT 
################################################################################################################
tx_rtt_temp_tbl = df_curr.filter(['IP', 'State', 'SurgeCommand', 'LGA', 'DATIMCode', 'Facility Name','TX_RTT', 'TX_RTT Captured', 
                                 '% TX_RTT Captured', 'TX_RTT Recaptured','% TX_RTT Recaptured', 'TX_RTT <3Months', 'TX_RTT 3_5Mths',
                                 'TX_RTT 6Months+', 'TX_RTT CD4 <200', 'TX_RTT CD4 ≥200','TX_RTT Unknown CD4'])

df_rtt_list = temp_table_curr[temp_table_curr['TX_RTT'] == 'Yes']

rtt_pbs_list = temp_table_curr[temp_table_curr['TX_RTT Captured'] == 'Yes']

rtt_cd4_list = temp_table_curr[temp_table_curr['TX_RTT_CD4_2'] == 'Yes']
################################################################################################################
# Manipulate the summary and patient tables for TX_PVLS 
################################################################################################################
tx_pvls_temp_tbl = df_curr.filter(['IP', 'State', 'SurgeCommand', 'LGA', 'DATIMCode', 'Facility Name','TX_CURR', 'Eligible for VL', 
            'VL sample taken in 1 year', 'TX_PVLS (D)', '% VL Coverage', 'TX_PVLS (N)', '% VL Suppression', 'Undetected VL <50', 
            '% Undetected', 'VL Eligible Not Bled', 'VL Awaiting Result','TX_PVLS (D) Captured', '% TX_PVLS (D) Captured', 
            'TX_PVLS (D) Recaptured', '% TX_PVLS (D) Recaptured'])

# Create IP Summary Table
tx_pvls_ip = tx_pvls_temp_tbl.groupby('IP', as_index=False)['Eligible for VL', 'VL sample taken in 1 year', 'TX_PVLS (D)', 'TX_PVLS (D) Captured', 
                                'TX_PVLS (D) Recaptured', 'TX_PVLS (N)', 'Undetected VL <50', 'VL Eligible Not Bled', 
                                'VL Awaiting Result'].sum()
# Numeric for line chart
tx_pvls_ip['% Coverage'] = np.round((tx_pvls_ip['TX_PVLS (D)'] / tx_pvls_ip['Eligible for VL']) * 100, decimals=1)
tx_pvls_ip['% Suppression'] = np.round((tx_pvls_ip['TX_PVLS (N)'] / tx_pvls_ip['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_ip['% Undetected'] = np.round((tx_pvls_ip['Undetected VL <50'] / tx_pvls_ip['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_ip['% TX_PVLS (D) Captured'] = np.round((tx_pvls_ip['TX_PVLS (D) Captured'] / tx_pvls_ip['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_ip['% TX_PVLS (D) Recaptured'] = np.round((tx_pvls_ip['TX_PVLS (D) Recaptured'] / tx_pvls_ip['TX_PVLS (D) Captured']) * 100, decimals=1)

# Filter and group by 'IP' and 'mmd'
grouped_pvls = temp_table_curr.groupby(['IP', 'mmd'], as_index=False)['Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
grouped_pvls['% Coverage'] = np.round((grouped_pvls['TX_PVLS (D)'] / grouped_pvls['Eligible for VL']) * 100, decimals=1)
grouped_pvls['% Suppression'] = np.round((grouped_pvls['TX_PVLS (N)'] / grouped_pvls['TX_PVLS (D)']) * 100, decimals=1)

# Create IP Summary Table By Age
tx_pvls_temp_tbl_all_age = temp_table_curr.groupby(['IP', 'AgeGroup'], as_index=False)['TX_CURR','Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
tx_pvls_temp_tbl_all_age.loc[:, '% Coverage'] = np.round((tx_pvls_temp_tbl_all_age['TX_PVLS (D)'] / tx_pvls_temp_tbl_all_age['Eligible for VL']) * 100, decimals=1)
tx_pvls_temp_tbl_all_age.loc[:, '% Suppression'] = np.round((tx_pvls_temp_tbl_all_age['TX_PVLS (N)'] / tx_pvls_temp_tbl_all_age['TX_PVLS (D)']) * 100, decimals=1)


tx_pvls_temp_tbl_age = temp_table_curr.groupby(['IP', 'AgeGroup', 'Sex'], as_index=False)['TX_CURR','Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
tx_pvls_temp_tbl_age['% Coverage'] = np.round((tx_pvls_temp_tbl_age['TX_PVLS (D)'] / tx_pvls_temp_tbl_age['Eligible for VL']) * 100, decimals=1)
tx_pvls_temp_tbl_age['% Suppression'] = np.round((tx_pvls_temp_tbl_age['TX_PVLS (N)'] / tx_pvls_temp_tbl_age['TX_PVLS (D)']) * 100, decimals=1)
# Replacing NaN with '0.0%'
tx_pvls_temp_tbl_age = tx_pvls_temp_tbl_age.replace('nan%', '0.0%', regex=True)

tx_pvls_temp_tbl_age_female = tx_pvls_temp_tbl_age[(tx_pvls_temp_tbl_age.Sex =="Female")]
tx_pvls_temp_tbl_age_male = tx_pvls_temp_tbl_age[(tx_pvls_temp_tbl_age.Sex =="Male")]

# Rename variables
tx_pvls_temp_tbl_age_female.rename(columns = {'TX_CURR':'TX_CURR (Female)','Eligible for VL':'Eligible (Female)', 'TX_PVLS (D)':'TX_PVLS (D, Female)', 
        'TX_PVLS (N)':'TX_PVLS (N, Female)', '% Coverage':'% Coverage (Female)','% Suppression':'% Suppression (Female)'}, inplace = True)

tx_pvls_temp_tbl_age_male.rename(columns = {'TX_CURR':'TX_CURR (Male)','Eligible for VL':'Eligible (Male)', 'TX_PVLS (D)':'TX_PVLS (D, Male)', 
                'TX_PVLS (N)':'TX_PVLS (N, Male)','% Coverage':'% Coverage (Male)','% Suppression':'% Suppression (Male)'}, inplace = True)

# Create IP-State VL Cascade Summary Table 1
tx_pvls_state_sum = df_curr.groupby('State', as_index=False)['TX_CURR', 'Eligible for VL', 'VL sample taken in 1 year', 'TX_PVLS (D)', 
                                'VL Awaiting Result', 'VL Eligible Not Bled','TX_PVLS (N)', 'Undetected VL <50'].sum()

tx_pvls_ip2=tx_pvls_state_sum.rename(columns = {'TX_CURR':'TX_CURR (a)','Eligible for VL':'Eligible for VL (b)', 'TX_PVLS (D)':'TX_PVLS (D) (d)', 
                                         'VL sample taken in 1 year':'VL samples taken in 1 year (c)', 'TX_PVLS (N)':'TX_PVLS (N) (g)',
                                         'VL Awaiting Result':'VL Awaiting Results (e)', 'VL Eligible Not Bled':'VL Eligible Not Bled (f)',
                                         'Undetected VL <50':'Undetected VL <50 (h)'})

tx_pvls_ip2['% VL Coverage =(d/b)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_ip2['TX_PVLS (D) (d)'] / 
                                                                     tx_pvls_ip2['Eligible for VL (b)']) * 100, decimals=1)))

tx_pvls_ip2['% Suppression =(g/d)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_ip2['TX_PVLS (N) (g)'] / 
                                                                     tx_pvls_ip2['TX_PVLS (D) (d)']) * 100, decimals=1)))

tx_pvls_ip2['% Undetected =(h/d)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_ip2['Undetected VL <50 (h)'] / 
                                                                     tx_pvls_ip2['TX_PVLS (D) (d)']) * 100, decimals=1)))

tx_pvls_ip2.loc[:, 'Projected VL Results =(d+e)'] = tx_pvls_ip2['TX_PVLS (D) (d)'] + tx_pvls_ip2['VL Awaiting Results (e)']

tx_pvls_ip2['% Projected VL Coverage =(d+e)/b'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_ip2['Projected VL Results =(d+e)'] / 
                                                                     tx_pvls_ip2['Eligible for VL (b)']) * 100, decimals=1)))

# Add a Total row
total_row = tx_pvls_ip2.sum(numeric_only=True)
total_row['State'] = 'CIHP'
total_row['% VL Coverage =(d/b)*100'] = str(np.round((total_row['TX_PVLS (D) (d)'] / total_row['Eligible for VL (b)']) * 100)) + "%"
total_row['% Suppression =(g/d)*100'] = str(np.round((total_row['TX_PVLS (N) (g)'] / total_row['TX_PVLS (D) (d)']) * 100)) + "%"
total_row['% Projected VL Coverage =(d+e)/b'] = str(np.round((total_row['Projected VL Results =(d+e)'] / total_row['Eligible for VL (b)']) * 100)) + "%"
total_row['% Undetected =(h/d)*100'] = str(np.round((total_row['Undetected VL <50 (h)'] / total_row['TX_PVLS (D) (d)']) * 100)) + "%"

tx_pvls_ip2b = tx_pvls_ip2.append(total_row, ignore_index=True)
# Reorder the columns
tx_pvls_ip3 = tx_pvls_ip2b[['State', 'TX_CURR (a)', 'Eligible for VL (b)', 'VL samples taken in 1 year (c)', 'TX_PVLS (D) (d)', 
                        '% VL Coverage =(d/b)*100', 'VL Awaiting Results (e)', 'VL Eligible Not Bled (f)', 'TX_PVLS (N) (g)', 
                        '% Suppression =(g/d)*100', 'Projected VL Results =(d+e)', '% Projected VL Coverage =(d+e)/b',
                        'Undetected VL <50 (h)', '% Undetected =(h/d)*100']]

####################### State ####################### State ####################### State #######################
# Create State Summary Table
tx_pvls_state = tx_pvls_temp_tbl.groupby('State', as_index=False)['Eligible for VL', 'VL sample taken in 1 year', 'TX_PVLS (D)', 'TX_PVLS (D) Captured', 
                                'TX_PVLS (D) Recaptured', 'TX_PVLS (N)', 'Undetected VL <50', 'VL Eligible Not Bled', 
                                'VL Awaiting Result'].sum()
# Numeric for line chart
tx_pvls_state['% Coverage'] = np.round((tx_pvls_state['TX_PVLS (D)'] / tx_pvls_state['Eligible for VL']) * 100, decimals=1)
tx_pvls_state['% Suppression'] = np.round((tx_pvls_state['TX_PVLS (N)'] / tx_pvls_state['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_state['% Undetected'] = np.round((tx_pvls_state['Undetected VL <50'] / tx_pvls_state['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_state['% TX_PVLS (D) Captured'] = np.round((tx_pvls_state['TX_PVLS (D) Captured'] / tx_pvls_state['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_state['% TX_PVLS (D) Recaptured'] = np.round((tx_pvls_state['TX_PVLS (D) Recaptured'] / tx_pvls_state['TX_PVLS (D) Captured']) * 100, decimals=1)

# Create State VL Cascade Summary Table 1 (From IP Table)
tx_pvls_state2 = tx_pvls_ip2[['State', 'TX_CURR (a)', 'Eligible for VL (b)', 'VL samples taken in 1 year (c)', 'TX_PVLS (D) (d)', 
                        '% VL Coverage =(d/b)*100', 'VL Awaiting Results (e)', 'VL Eligible Not Bled (f)', 'TX_PVLS (N) (g)', 
                        '% Suppression =(g/d)*100', 'Projected VL Results =(d+e)', '% Projected VL Coverage =(d+e)/b',
                        'Undetected VL <50 (h)', '% Undetected =(h/d)*100']]

# Filter and group by 'State' and 'mmd'
grouped_pvls_state = temp_table_curr.groupby(['State', 'mmd'], as_index=False)['Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
grouped_pvls_state['% Coverage'] = np.round((grouped_pvls_state['TX_PVLS (D)'] / grouped_pvls_state['Eligible for VL']) * 100, decimals=1)
grouped_pvls_state['% Suppression'] = np.round((grouped_pvls_state['TX_PVLS (N)'] / grouped_pvls_state['TX_PVLS (D)']) * 100, decimals=1)

# Create State Summary Table By Age
pvls_temp_tbl_all_age_state = temp_table_curr.groupby(['State', 'LGA', 'SurgeCommand', 'AgeGroup'], as_index=False)['TX_CURR','Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
pvls_temp_tbl_all_age_state.loc[:, '% Coverage'] = np.round((pvls_temp_tbl_all_age_state['TX_PVLS (D)'] / pvls_temp_tbl_all_age_state['Eligible for VL']) * 100, decimals=1)
pvls_temp_tbl_all_age_state.loc[:, '% Suppression'] = np.round((pvls_temp_tbl_all_age_state['TX_PVLS (N)'] / pvls_temp_tbl_all_age_state['TX_PVLS (D)']) * 100, decimals=1)

pvls_temp_tbl_age_state = temp_table_curr.groupby(['State', 'AgeGroup', 'Sex'], as_index=False)['TX_CURR','Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
pvls_temp_tbl_age_state['% Coverage'] = np.round((pvls_temp_tbl_age_state['TX_PVLS (D)'] / pvls_temp_tbl_age_state['Eligible for VL']) * 100, decimals=1)
pvls_temp_tbl_age_state['% Suppression'] = np.round((pvls_temp_tbl_age_state['TX_PVLS (N)'] / pvls_temp_tbl_age_state['TX_PVLS (D)']) * 100, decimals=1)
# Replacing NaN with '0.0%'
pvls_temp_tbl_age_state = pvls_temp_tbl_age_state.replace('nan%', '0.0%', regex=True)

pvls_temp_tbl_age_state_female = pvls_temp_tbl_age_state[(pvls_temp_tbl_age_state.Sex =="Female")]
pvls_temp_tbl_age_state_male = pvls_temp_tbl_age_state[(pvls_temp_tbl_age_state.Sex =="Male")]

# Rename variables
pvls_temp_tbl_age_state_female.rename(columns = {'TX_CURR':'TX_CURR (Female)','Eligible for VL':'Eligible (Female)', 'TX_PVLS (D)':'TX_PVLS (D, Female)', 
        'TX_PVLS (N)':'TX_PVLS (N, Female)', '% Coverage':'% Coverage (Female)','% Suppression':'% Suppression (Female)'}, inplace = True)

pvls_temp_tbl_age_state_male.rename(columns = {'TX_CURR':'TX_CURR (Male)','Eligible for VL':'Eligible (Male)', 'TX_PVLS (D)':'TX_PVLS (D, Male)', 
                'TX_PVLS (N)':'TX_PVLS (N, Male)','% Coverage':'% Coverage (Male)','% Suppression':'% Suppression (Male)'}, inplace = True)

####################### LGA ####################### LGA ####################### LGA #######################
# Create LGA Summary Table
tx_pvls_lga = tx_pvls_temp_tbl.groupby('LGA', as_index=False)['TX_CURR', 'Eligible for VL', 'VL sample taken in 1 year', 'TX_PVLS (D)', 'TX_PVLS (D) Captured', 
                                'TX_PVLS (D) Recaptured', 'TX_PVLS (N)', 'Undetected VL <50', 'VL Eligible Not Bled', 
                                'VL Awaiting Result'].sum()
# Numeric for line chart
tx_pvls_lga['% Coverage'] = np.round((tx_pvls_lga['TX_PVLS (D)'] / tx_pvls_lga['Eligible for VL']) * 100, decimals=1)
tx_pvls_lga['% Suppression'] = np.round((tx_pvls_lga['TX_PVLS (N)'] / tx_pvls_lga['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_lga['% Undetected'] = np.round((tx_pvls_lga['Undetected VL <50'] / tx_pvls_lga['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_lga['% TX_PVLS (D) Captured'] = np.round((tx_pvls_lga['TX_PVLS (D) Captured'] / tx_pvls_lga['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_lga['% TX_PVLS (D) Recaptured'] = np.round((tx_pvls_lga['TX_PVLS (D) Recaptured'] / tx_pvls_lga['TX_PVLS (D) Captured']) * 100, decimals=1)

tx_pvls_lga2=tx_pvls_lga.rename(columns = {'TX_CURR':'TX_CURR (a)','Eligible for VL':'Eligible for VL (b)', 'TX_PVLS (D)':'TX_PVLS (D) (d)', 
                                         'VL sample taken in 1 year':'VL samples taken in 1 year (c)', 'TX_PVLS (N)':'TX_PVLS (N) (g)',
                                         'VL Awaiting Result':'VL Awaiting Results (e)', 'VL Eligible Not Bled':'VL Eligible Not Bled (f)',
                                         'Undetected VL <50':'Undetected VL <50 (h)', '% Coverage':'% VL Coverage =(d/b)*100',
                                         '% Suppression':'% Suppression =(g/d)*100','% Undetected':'% Undetected =(h/d)*100'})

tx_pvls_lga2['% VL Coverage =(d/b)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_lga2['TX_PVLS (D) (d)'] / 
                                                                     tx_pvls_lga2['Eligible for VL (b)']) * 100, decimals=1)))

tx_pvls_lga2['% Suppression =(g/d)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_lga2['TX_PVLS (N) (g)'] / 
                                                                     tx_pvls_lga2['TX_PVLS (D) (d)']) * 100, decimals=1)))

tx_pvls_lga2['% Undetected =(h/d)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_lga2['Undetected VL <50 (h)'] / 
                                                                     tx_pvls_lga2['TX_PVLS (D) (d)']) * 100, decimals=1)))

tx_pvls_lga2.loc[:, 'Projected VL Results =(d+e)'] = tx_pvls_lga2['TX_PVLS (D) (d)'] + tx_pvls_lga2['VL Awaiting Results (e)']

tx_pvls_lga2['% Projected VL Coverage =(d+e)/b'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_lga2['Projected VL Results =(d+e)'] / 
                                                                     tx_pvls_lga2['Eligible for VL (b)']) * 100, decimals=1)))

# Reorder the columns
tx_pvls_lga3 = tx_pvls_lga2[['LGA', 'TX_CURR (a)', 'Eligible for VL (b)', 'VL samples taken in 1 year (c)', 'TX_PVLS (D) (d)', 
                        '% VL Coverage =(d/b)*100', 'VL Awaiting Results (e)', 'VL Eligible Not Bled (f)', 'TX_PVLS (N) (g)', 
                        '% Suppression =(g/d)*100', 'Projected VL Results =(d+e)', '% Projected VL Coverage =(d+e)/b',
                        'Undetected VL <50 (h)', '% Undetected =(h/d)*100']]

# Filter and group by 'LGA' and 'mmd'
grouped_pvls_lga = temp_table_curr.groupby(['LGA', 'mmd'], as_index=False)['Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
grouped_pvls_lga['% Coverage'] = np.round((grouped_pvls_lga['TX_PVLS (D)'] / grouped_pvls_lga['Eligible for VL']) * 100, decimals=1)
grouped_pvls_lga['% Suppression'] = np.round((grouped_pvls_lga['TX_PVLS (N)'] / grouped_pvls_lga['TX_PVLS (D)']) * 100, decimals=1)

# Create LGA Summary Table By Age
pvls_temp_tbl_all_age_lga = temp_table_curr.groupby(['LGA', 'FacilityName', 'AgeGroup'], as_index=False)['TX_CURR','Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
pvls_temp_tbl_all_age_lga.loc[:, '% Coverage'] = np.round((pvls_temp_tbl_all_age_lga['TX_PVLS (D)'] / pvls_temp_tbl_all_age_lga['Eligible for VL']) * 100, decimals=1)
pvls_temp_tbl_all_age_lga.loc[:, '% Suppression'] = np.round((pvls_temp_tbl_all_age_lga['TX_PVLS (N)'] / pvls_temp_tbl_all_age_lga['TX_PVLS (D)']) * 100, decimals=1)

pvls_temp_tbl_age_lga = temp_table_curr.groupby(['LGA', 'AgeGroup', 'Sex'], as_index=False)['TX_CURR','Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
pvls_temp_tbl_age_lga['% Coverage'] = np.round((pvls_temp_tbl_age_lga['TX_PVLS (D)'] / pvls_temp_tbl_age_lga['Eligible for VL']) * 100, decimals=1)
pvls_temp_tbl_age_lga['% Suppression'] = np.round((pvls_temp_tbl_age_lga['TX_PVLS (N)'] / pvls_temp_tbl_age_lga['TX_PVLS (D)']) * 100, decimals=1)
# Replacing NaN with '0.0%'
pvls_temp_tbl_age_lga = pvls_temp_tbl_age_lga.replace('nan%', '0.0%', regex=True)

pvls_temp_tbl_age_lga_female = pvls_temp_tbl_age_lga[(pvls_temp_tbl_age_lga.Sex =="Female")]
pvls_temp_tbl_age_lga_male = pvls_temp_tbl_age_lga[(pvls_temp_tbl_age_lga.Sex =="Male")]

# Rename variables
pvls_temp_tbl_age_lga_female.rename(columns = {'TX_CURR':'TX_CURR (Female)','Eligible for VL':'Eligible (Female)', 'TX_PVLS (D)':'TX_PVLS (D, Female)', 
        'TX_PVLS (N)':'TX_PVLS (N, Female)', '% Coverage':'% Coverage (Female)','% Suppression':'% Suppression (Female)'}, inplace = True)

pvls_temp_tbl_age_lga_male.rename(columns = {'TX_CURR':'TX_CURR (Male)','Eligible for VL':'Eligible (Male)', 'TX_PVLS (D)':'TX_PVLS (D, Male)', 
                'TX_PVLS (N)':'TX_PVLS (N, Male)','% Coverage':'% Coverage (Male)','% Suppression':'% Suppression (Male)'}, inplace = True)
####################### SurgeCommand ####################### SurgeCommand ####################### SurgeCommand #######################
# Create SurgeCommand Summary Table
tx_pvls_comd = tx_pvls_temp_tbl.groupby('SurgeCommand', as_index=False)['TX_CURR', 'Eligible for VL', 'VL sample taken in 1 year', 'TX_PVLS (D)', 'TX_PVLS (D) Captured', 
                                'TX_PVLS (D) Recaptured', 'TX_PVLS (N)', 'Undetected VL <50', 'VL Eligible Not Bled', 
                                'VL Awaiting Result'].sum()
# Numeric for line chart
tx_pvls_comd['% Coverage'] = np.round((tx_pvls_comd['TX_PVLS (D)'] / tx_pvls_comd['Eligible for VL']) * 100, decimals=1)
tx_pvls_comd['% Suppression'] = np.round((tx_pvls_comd['TX_PVLS (N)'] / tx_pvls_comd['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_comd['% Undetected'] = np.round((tx_pvls_comd['Undetected VL <50'] / tx_pvls_comd['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_comd['% TX_PVLS (D) Captured'] = np.round((tx_pvls_comd['TX_PVLS (D) Captured'] / tx_pvls_comd['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_comd['% TX_PVLS (D) Recaptured'] = np.round((tx_pvls_comd['TX_PVLS (D) Recaptured'] / tx_pvls_comd['TX_PVLS (D) Captured']) * 100, decimals=1)

tx_pvls_comd2=tx_pvls_comd.rename(columns = {'TX_CURR':'TX_CURR (a)','Eligible for VL':'Eligible for VL (b)', 'TX_PVLS (D)':'TX_PVLS (D) (d)', 
                                         'VL sample taken in 1 year':'VL samples taken in 1 year (c)', 'TX_PVLS (N)':'TX_PVLS (N) (g)',
                                         'VL Awaiting Result':'VL Awaiting Results (e)', 'VL Eligible Not Bled':'VL Eligible Not Bled (f)',
                                         'Undetected VL <50':'Undetected VL <50 (h)', '% Coverage':'% VL Coverage =(d/b)*100',
                                         '% Suppression':'% Suppression =(g/d)*100','% Undetected':'% Undetected =(h/d)*100'})

tx_pvls_comd2['% VL Coverage =(d/b)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_comd2['TX_PVLS (D) (d)'] / 
                                                                     tx_pvls_comd2['Eligible for VL (b)']) * 100, decimals=1)))

tx_pvls_comd2['% Suppression =(g/d)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_comd2['TX_PVLS (N) (g)'] / 
                                                                     tx_pvls_comd2['TX_PVLS (D) (d)']) * 100, decimals=1)))

tx_pvls_comd2['% Undetected =(h/d)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_comd2['Undetected VL <50 (h)'] / 
                                                                     tx_pvls_comd2['TX_PVLS (D) (d)']) * 100, decimals=1)))

tx_pvls_comd2.loc[:, 'Projected VL Results =(d+e)'] = tx_pvls_comd2['TX_PVLS (D) (d)'] + tx_pvls_comd2['VL Awaiting Results (e)']

tx_pvls_comd2['% Projected VL Coverage =(d+e)/b'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_comd2['Projected VL Results =(d+e)'] / 
                                                                     tx_pvls_comd2['Eligible for VL (b)']) * 100, decimals=1)))

# Reorder the columns
tx_pvls_comd3 = tx_pvls_comd2[['SurgeCommand', 'TX_CURR (a)', 'Eligible for VL (b)', 'VL samples taken in 1 year (c)', 'TX_PVLS (D) (d)', 
                        '% VL Coverage =(d/b)*100', 'VL Awaiting Results (e)', 'VL Eligible Not Bled (f)', 'TX_PVLS (N) (g)', 
                        '% Suppression =(g/d)*100', 'Projected VL Results =(d+e)', '% Projected VL Coverage =(d+e)/b',
                        'Undetected VL <50 (h)', '% Undetected =(h/d)*100']]

# Filter and group by 'SurgeCommand' and 'mmd'
grouped_pvls_comd = temp_table_curr.groupby(['SurgeCommand', 'mmd'], as_index=False)['Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
grouped_pvls_comd['% Coverage'] = np.round((grouped_pvls_comd['TX_PVLS (D)'] / grouped_pvls_comd['Eligible for VL']) * 100, decimals=1)
grouped_pvls_comd['% Suppression'] = np.round((grouped_pvls_comd['TX_PVLS (N)'] / grouped_pvls_comd['TX_PVLS (D)']) * 100, decimals=1)

# Create SurgeCommand Summary Table By Age
pvls_temp_tbl_all_age_comd = temp_table_curr.groupby(['SurgeCommand', 'FacilityName', 'AgeGroup'], as_index=False)['TX_CURR','Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
pvls_temp_tbl_all_age_comd.loc[:, '% Coverage'] = np.round((pvls_temp_tbl_all_age_comd['TX_PVLS (D)'] / pvls_temp_tbl_all_age_comd['Eligible for VL']) * 100, decimals=1)
pvls_temp_tbl_all_age_comd.loc[:, '% Suppression'] = np.round((pvls_temp_tbl_all_age_comd['TX_PVLS (N)'] / pvls_temp_tbl_all_age_comd['TX_PVLS (D)']) * 100, decimals=1)

pvls_temp_tbl_age_comd = temp_table_curr.groupby(['SurgeCommand', 'AgeGroup', 'Sex'], as_index=False)['TX_CURR','Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
pvls_temp_tbl_age_comd['% Coverage'] = np.round((pvls_temp_tbl_age_comd['TX_PVLS (D)'] / pvls_temp_tbl_age_comd['Eligible for VL']) * 100, decimals=1)
pvls_temp_tbl_age_comd['% Suppression'] = np.round((pvls_temp_tbl_age_comd['TX_PVLS (N)'] / pvls_temp_tbl_age_comd['TX_PVLS (D)']) * 100, decimals=1)
# Replacing NaN with '0.0%'
pvls_temp_tbl_age_comd = pvls_temp_tbl_age_comd.replace('nan%', '0.0%', regex=True)

pvls_temp_tbl_age_comd_female = pvls_temp_tbl_age_comd[(pvls_temp_tbl_age_comd.Sex =="Female")]
pvls_temp_tbl_age_comd_male = pvls_temp_tbl_age_comd[(pvls_temp_tbl_age_comd.Sex =="Male")]

# Rename variables
pvls_temp_tbl_age_comd_female.rename(columns = {'TX_CURR':'TX_CURR (Female)','Eligible for VL':'Eligible (Female)', 'TX_PVLS (D)':'TX_PVLS (D, Female)', 
        'TX_PVLS (N)':'TX_PVLS (N, Female)', '% Coverage':'% Coverage (Female)','% Suppression':'% Suppression (Female)'}, inplace = True)

pvls_temp_tbl_age_comd_male.rename(columns = {'TX_CURR':'TX_CURR (Male)','Eligible for VL':'Eligible (Male)', 'TX_PVLS (D)':'TX_PVLS (D, Male)', 
                'TX_PVLS (N)':'TX_PVLS (N, Male)','% Coverage':'% Coverage (Male)','% Suppression':'% Suppression (Male)'}, inplace = True)

##################### Facility ####################### Facility ####################### Facility #######################
# Create Facility Name Summary Table
tx_pvls_fac = tx_pvls_temp_tbl.groupby('Facility Name', as_index=False)['TX_CURR', 'Eligible for VL', 'VL sample taken in 1 year', 'TX_PVLS (D)', 'TX_PVLS (D) Captured', 
                                'TX_PVLS (D) Recaptured', 'TX_PVLS (N)', 'Undetected VL <50', 'VL Eligible Not Bled', 
                                'VL Awaiting Result'].sum()
# Numeric for line chart
tx_pvls_fac['% Coverage'] = np.round((tx_pvls_fac['TX_PVLS (D)'] / tx_pvls_fac['Eligible for VL']) * 100, decimals=1)
tx_pvls_fac['% Suppression'] = np.round((tx_pvls_fac['TX_PVLS (N)'] / tx_pvls_fac['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_fac['% Undetected'] = np.round((tx_pvls_fac['Undetected VL <50'] / tx_pvls_fac['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_fac['% TX_PVLS (D) Captured'] = np.round((tx_pvls_fac['TX_PVLS (D) Captured'] / tx_pvls_fac['TX_PVLS (D)']) * 100, decimals=1)
tx_pvls_fac['% TX_PVLS (D) Recaptured'] = np.round((tx_pvls_fac['TX_PVLS (D) Recaptured'] / tx_pvls_fac['TX_PVLS (D) Captured']) * 100, decimals=1)

tx_pvls_fac2=tx_pvls_fac.rename(columns = {'Facility Name':'Facility','TX_CURR':'TX_CURR (a)','Eligible for VL':'Eligible for VL (b)', 'TX_PVLS (D)':'TX_PVLS (D) (d)', 
                                         'VL sample taken in 1 year':'VL samples taken in 1 year (c)', 'TX_PVLS (N)':'TX_PVLS (N) (g)',
                                         'VL Awaiting Result':'VL Awaiting Results (e)', 'VL Eligible Not Bled':'VL Eligible Not Bled (f)',
                                         'Undetected VL <50':'Undetected VL <50 (h)', '% Coverage':'% VL Coverage =(d/b)*100',
                                         '% Suppression':'% Suppression =(g/d)*100','% Undetected':'% Undetected =(h/d)*100'})

tx_pvls_fac2['% VL Coverage =(d/b)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_fac2['TX_PVLS (D) (d)'] / 
                                                                     tx_pvls_fac2['Eligible for VL (b)']) * 100, decimals=1)))

tx_pvls_fac2['% Suppression =(g/d)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_fac2['TX_PVLS (N) (g)'] / 
                                                                     tx_pvls_fac2['TX_PVLS (D) (d)']) * 100, decimals=1)))

tx_pvls_fac2['% Undetected =(h/d)*100'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_fac2['Undetected VL <50 (h)'] / 
                                                                     tx_pvls_fac2['TX_PVLS (D) (d)']) * 100, decimals=1)))

tx_pvls_fac2.loc[:, 'Projected VL Results =(d+e)'] = tx_pvls_fac2['TX_PVLS (D) (d)'] + tx_pvls_fac2['VL Awaiting Results (e)']

tx_pvls_fac2['% Projected VL Coverage =(d+e)/b'] = list(map(lambda x:str(x)+"%", np.round((tx_pvls_fac2['Projected VL Results =(d+e)'] / 
                                                                     tx_pvls_fac2['Eligible for VL (b)']) * 100, decimals=1)))

# Reorder the columns
tx_pvls_fac3 = tx_pvls_fac2[['Facility', 'TX_CURR (a)', 'Eligible for VL (b)', 'VL samples taken in 1 year (c)', 'TX_PVLS (D) (d)', 
                        '% VL Coverage =(d/b)*100', 'VL Awaiting Results (e)', 'VL Eligible Not Bled (f)', 'TX_PVLS (N) (g)', 
                        '% Suppression =(g/d)*100', 'Projected VL Results =(d+e)', '% Projected VL Coverage =(d+e)/b',
                        'Undetected VL <50 (h)', '% Undetected =(h/d)*100']]

# Filter and group by 'Facility' and 'mmd'
grouped_pvls_fac = temp_table_curr.groupby(['FacilityName', 'mmd'], as_index=False)['Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
grouped_pvls_fac['% Coverage'] = np.round((grouped_pvls_fac['TX_PVLS (D)'] / grouped_pvls_fac['Eligible for VL']) * 100, decimals=1)
grouped_pvls_fac['% Suppression'] = np.round((grouped_pvls_fac['TX_PVLS (N)'] / grouped_pvls_fac['TX_PVLS (D)']) * 100, decimals=1)
# Create Facility Summary Table By Age
pvls_temp_tbl_all_age_fac = temp_table_curr.groupby(['FacilityName', 'AgeGroup'], as_index=False)['TX_CURR','Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
pvls_temp_tbl_all_age_fac.loc[:, '% Coverage'] = np.round((pvls_temp_tbl_all_age_fac['TX_PVLS (D)'] / pvls_temp_tbl_all_age_fac['Eligible for VL']) * 100, decimals=1)
pvls_temp_tbl_all_age_fac.loc[:, '% Suppression'] = np.round((pvls_temp_tbl_all_age_fac['TX_PVLS (N)'] / pvls_temp_tbl_all_age_fac['TX_PVLS (D)']) * 100, decimals=1)

pvls_temp_tbl_age_fac = temp_table_curr.groupby(['FacilityName', 'AgeGroup', 'Sex'], as_index=False)['TX_CURR','Eligible for VL', 'TX_PVLS (D)', 'TX_PVLS (N)'].count()
pvls_temp_tbl_age_fac['% Coverage'] = np.round((pvls_temp_tbl_age_fac['TX_PVLS (D)'] / pvls_temp_tbl_age_fac['Eligible for VL']) * 100, decimals=1)
pvls_temp_tbl_age_fac['% Suppression'] = np.round((pvls_temp_tbl_age_fac['TX_PVLS (N)'] / pvls_temp_tbl_age_fac['TX_PVLS (D)']) * 100, decimals=1)
# Replacing NaN with '0.0%'
pvls_temp_tbl_age_fac = pvls_temp_tbl_age_fac.replace('nan%', '0.0%', regex=True)

pvls_temp_tbl_age_fac_female = pvls_temp_tbl_age_fac[(pvls_temp_tbl_age_fac.Sex =="Female")]
pvls_temp_tbl_age_fac_male = pvls_temp_tbl_age_fac[(pvls_temp_tbl_age_fac.Sex =="Male")]

# Rename variables
pvls_temp_tbl_age_fac_female.rename(columns = {'TX_CURR':'TX_CURR (Female)','Eligible for VL':'Eligible (Female)', 'TX_PVLS (D)':'TX_PVLS (D, Female)', 
        'TX_PVLS (N)':'TX_PVLS (N, Female)', '% Coverage':'% Coverage (Female)','% Suppression':'% Suppression (Female)'}, inplace = True)

pvls_temp_tbl_age_fac_male.rename(columns = {'TX_CURR':'TX_CURR (Male)','Eligible for VL':'Eligible (Male)', 'TX_PVLS (D)':'TX_PVLS (D, Male)', 
                'TX_PVLS (N)':'TX_PVLS (N, Male)','% Coverage':'% Coverage (Male)','% Suppression':'% Suppression (Male)'}, inplace = True)
################################################################################################################
# Manipulate the summary and patient tables for PBFW PVLS 
################################################################################################################
pbfw_pvls_temp_tbl = df_curr.filter(['IP', 'State', 'SurgeCommand', 'LGA', 'DATIMCode', 'Facility Name','PBFW TX_CURR', 'PBFW VL Eligible', 
                                    'PBFW VLS (D)', '% PBFW VL Coverage', 'PBFW VLS (N)', '% PBFW VL Suppression', 'PBFW VLS (D) Captured',
                                    '% PBFW VLS (D) Captured', 'PBFW VLS (D) Recaptured', '% PBFW VLS (D) Recaptured'])
df_pbfw_list = temp_table_curr[temp_table_curr['PBFW TX_CURR'] == 'Yes']

pw_pvls_temp_tbl = df_curr.filter(['IP', 'State', 'SurgeCommand', 'LGA', 'DATIMCode', 'Facility Name','PW TX_CURR', 'PW VL Eligible', 
                                    'PW VLS (D)', '% PW VL Coverage', 'PW VLS (N)', '% PW VL Suppression', 'PW VLS (D) Captured',
                                    '% PW VLS (D) Captured', 'PW VLS (D) Recaptured', '% PW VLS (D) Recaptured'])

bfw_pvls_temp_tbl = df_curr.filter(['IP', 'State', 'SurgeCommand', 'LGA', 'DATIMCode', 'Facility Name','BFW TX_CURR', 'BFW VL Eligible', 
                                    'BFW VLS (D)', '% BFW VL Coverage', 'BFW VLS (N)', '% BFW VL Suppression', 'BFW VLS (D) Captured',
                                    '% BFW VLS (D) Captured', 'BFW VLS (D) Recaptured', '% BFW VLS (D) Recaptured'])
