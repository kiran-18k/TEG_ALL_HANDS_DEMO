# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 15:08:57 2022

@author: Kiran
"""

import pandas as pd
import numpy as np
from functools import reduce
import re
from scipy.stats import zscore
import warnings
warnings.filterwarnings('ignore')

#%%
path = 'C:/Users/Mattam.Bhanu/Desktop/Trended_BDA/FY22Q3/'
qc='C:/Users/Mattam.Bhanu/Desktop/Trended_BDA/FY22Q3/QC/QC_HC/'
output='C:/Users/Mattam.Bhanu/Desktop/Trended_BDA/FY22Q3/Output/output_HC/'
cust_path = 'C:/Users/Mattam.Bhanu/Desktop/Trended_BDA/FY22Q3/Cust_agg/'
pos_path = 'C:/Users/Mattam.Bhanu/Desktop/Trended_BDA/FY22Q3/Pos/PS/'
bda_path = 'C:/Users/Mattam.Bhanu/Desktop/Trended_BDA/FY22Q3/Coeff_DB/'
#upc_path = 'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Pricing/FY22Q3/POS/UPC/'

#%%
# #### Mapping Files

# Assign spreadsheet filename to `file`
mappings = 'Mapping BDA_PPL_FY21Q2.xlsx'

# Load spreadsheet
map_xl = pd.ExcelFile(path+'Mapping Files/'+mappings)

# Print the sheet names
print(map_xl.sheet_names)

# Load a sheet into respective DataFrames
geog_map = map_xl.parse('Geography Map')
time_map = map_xl.parse('Time Periods Map')
prod_map = map_xl.parse('BDA Product Map')
prod_map_glad = map_xl.parse('BDA Prodmap Glad')
ppl_map = map_xl.parse('PPL')
pos_map = map_xl.parse('POS Mapping')
pos_map = pos_map[pos_map['Comment']=='Include']
prod_map = prod_map[prod_map['sbu']=='Home Care']

#%%
# POS file for HC
iri_df = pd.read_excel(pos_path + 'HC.xlsx', skiprows = 1)
iri_df['Geography'] = iri_df['Geography'].str.upper()
iri_df = iri_df[iri_df['Geography'].isin(['TOTAL US - FOOD','WALMART CORP-RMA - WALMART',
                            'TARGET CORP-RMA - MASS',"SAM'S CORP-RMA - CLUB"])]
iri_df['Geography'] = iri_df['Geography'].str.upper().map(geog_map.set_index('Geography_Name')['Geography'])

#%%
pos_hc = iri_df.copy()
pos_hc = pos_hc[['Geography','Product','Product Key']].drop_duplicates() 

# Map Product 
pos_hc['Sub_Brand'] = pos_hc['Product'].map(pos_map.set_index('Product Name')['Subbrand Elasticity File'])
pos_hc['Brand'] = pos_hc['Product'].map(pos_map.set_index('Product Name')['Brand Elasticity File'])
pos_hc['SBU'] = 'Home Care'
pos_hc['Division'] = 'Cleaning'

pos_hc = pos_hc[pos_hc['Sub_Brand'].notnull()]
pos_hc.to_csv(qc+'pos_hc.csv')

#%%
ppl_map = ppl_map[['Brand Elasticity File','Subbrand Elasticity File']].drop_duplicates()
pos_hc = ppl_map.merge(pos_hc,left_on=['Brand Elasticity File','Subbrand Elasticity File'],right_on=['Brand','Sub_Brand'],how='inner')
pos_hc.to_csv(qc+'pos_hc1.csv')

#%%
iri_df = iri_df[iri_df['Standard Hierarchy Level'].isin(['FLAVOR/SCENT GROUPING_THC_H1_3'])].reset_index(drop=True)
iri_df['SCBV_new'] = iri_df.apply(lambda x: x['Baseline Volume']/(x['Volume Sales']/x['Stat Case Volume']) if
(pd.isna(x['Stat Case Baseline Volume']) and (~pd.isna(x['Stat Case Volume']))) else x['Stat Case Baseline Volume'],axis=1)
iri_df.drop(columns=['Stat Case Baseline Volume'],axis=1,inplace=True)
iri_df.rename(columns={'SCBV_new':'Stat Case Baseline Volume'},inplace=True)
iri_df = iri_df[iri_df['Stat Case Baseline Volume'].notnull()].reset_index(drop=True)

brands = ['CLOROX','PINE SOL','FORMULA 409','S O S','LIQUID PLUMR','TILEX','GREEN WORKS']
iri_df['Clx_Comp'] = np.where(iri_df['Clorox Brand Value'].isin(brands),"Clorox","Competitor")

#%%
iri_df['new'] = iri_df['Product Key'].str.split(':')
iri_df['len'] = iri_df['new'].str.len()
iri_df1 = iri_df[iri_df['len'] == iri_df['len'].max()]
iri_df2 = iri_df[iri_df['len'] != iri_df['len'].max()]

#%%
#Size Correction and missing size imputation
iri_df1['Clorox Size Value2'] = iri_df1.apply(lambda x: x['Clorox Size Value'] 
                                    if x['Clorox Sub Category Value'] in ['WIPES', 'DILUTABLES', 'BATHROOM CLEANERS', 'SPRAY CLEANERS','DRAIN CARE']
                                    else (x['Clorox Size Value'] if (x['Clorox Sub Category Value'] == 'TOILET BOWL CLEANERS' 
                                                                 and x['Clorox Segment Value'] in ['AUTOMATIC TB CLEANERS', 'MANUAL TB CLEANERS'])
                                                                 else x['Clorox Pack Type Value']), axis = 1)


#%%
def cat_val(a, b):
    if a == "SPRAY CLEANERS":
        cat = "GENERAL SPRAY CLEANERS"
        return cat
    elif (a == "BATHROOM CLEANERS" and b in ['CONVENIENCE BATH CLEANERS','DAILY SHOWER CLEANERS',
                                           'MILDEW REMOVERS','GENERAL BATHROOM CLEANERS']):
        cat = "SPECIAL SPRAY CLEANERS"
        return cat
    elif a == "WIPES":
        cat = "WIPES"
        return cat
    elif a == "DILUTABLES":
        cat = "DILUTABLES"
        return cat
    elif b == "AUTOMATIC TB CLEANERS":
        cat = "AUTOMATIC TB CLEANERS"
        return cat
    elif b == "CONVENIENCE TB CLEANERS":
        cat = "CONVENIENCE TB CLEANERS"
        return cat
    elif b == "MANUAL TB CLEANERS":
        cat = "MANUAL TB CLEANERS"
        return cat
    elif a == "DRAIN CARE":
        cat = "DRAIN CARE"
        return cat

#%%
iri_df1["Category"] = iri_df1.apply(lambda x: cat_val(x['Clorox Sub Category Value'], x['Clorox Segment Value']), axis = 1)

#########
iri_df1["BU"] = 'HOME CARE'

#########
iri_df2 = iri_df1[iri_df1['Category'].isnull()==False]

#%%
iri_df2['size3'] = iri_df2.apply(lambda x: str(round(x['Volume Sales']/x['Unit Sales']))+ " CT" 
                                           if x['Category'] in ['CONVENIENCE TB CLEANERS', 'AUTOMATIC TB CLEANERS', 'WIPES']
                                           else str(round(x['Volume Sales']/x['Unit Sales']))+ " OZ", axis = 1)

iri_df2['Size_Final'] = iri_df2.apply(lambda x: x['size3'] if pd.isnull(x['Clorox Size Value2']) 
                                           else x['Clorox Size Value2'], axis = 1)

iri_df2 = iri_df2.drop(columns = ['size3','Clorox Size Value2'], axis = 1)
iri_df2.to_csv(qc+'iri_df2.csv')

#%%
# Size Correction
def size_corr(a, b):
    if a == "DILUTABLES" and b!= 'ALL OTHER SIZE':
        size = int(np.round(float(b.split()[0])+0.01))
        return(str(size)+" OZ")
        
    elif a == "GENERAL SPRAY CLEANERS" and b!= 'ALL OTHER SIZE' and float(b.split()[0])>=1:
        size = int(np.round(float(b.split()[0])+0.01))
        return(str(size)+" OZ")
        
    elif a == "MANUAL TB CLEANERS" and b!= 'ALL OTHER SIZE' and float(b.split()[0])>1:
        size = int(np.round(float(b.split()[0])+0.01))
        return(str(size)+" OZ")
        
    elif a == "SPECIAL SPRAY CLEANERS" and b!= 'ALL OTHER SIZE' and float(b.split()[0])>1:
        size = int(np.round(float(b.split()[0])+0.01))
        return(str(size)+" OZ")
    else:
        return(b)

#%%
iri_df3 = iri_df2.copy()
iri_df3["Size3"] = iri_df3.apply(lambda x: size_corr(x['Category'], x['Size_Final']), axis = 1)
iri_df3 = iri_df3.drop(columns = ['Size_Final'], axis = 1)
iri_df3 = iri_df3.rename(columns={'Size3':'Size_Final'})
iri_df3['size'] = iri_df3['Size_Final'].str.extract('(\d*\.\d+|\d+)').astype(float)
iri_df3 = iri_df3.rename(columns={'Time':'Refresh Period'})

## POS file is ready after all the size corrections and POC_HC file preparation

#%%
# Preparing Coefficient DB file for Merge:

#--BDA file
# Assign spreadsheet filename to `file`
file = 'CoefDB - All Total US FY19Q4.xlsx'
xl = pd.ExcelFile(bda_path+file)

# Print the sheet names
print(xl.sheet_names)

# In[2]:
# Load a sheet into a DataFrame bda_coeff_raw
bda_coeff_raw = xl.parse('CoefDb_ All Total US FY19Q4')
bda_raw_all = bda_coeff_raw[['model_source','Model_Period_End','catlib','Product_Level','Product_Name_Modeled','Product_Name_Current',
'Geography_Name','Geography_Level','Base_Price_Elasticity','Promo_Price_Elasticity','Base_Statcase_Volume','iriprod','prodkey']].drop_duplicates().reset_index(drop=True)
bda_raw_all = bda_raw_all.replace('NULL', np.nan, regex=True)
bda_raw_all['Product_Name_Modeled']= bda_raw_all['Product_Name_Modeled'].str.upper()
bda_raw_all.to_csv(qc+'bda_raw_all.csv')
    
#%%
# HC catlibs separated for automation. Check if new catlib available for HC
final_db1 = bda_raw_all[bda_raw_all['catlib'].isin(['CW','LP','S1','S2','S3','SP', 'SD'
            'SB','SS','SW','T2','TA','TM','TW','TX','W2','UR']) & bda_raw_all['Product_Level'].isin(['S','X','K','Z','I'])]
final_db1 = final_db1.drop_duplicates()
final_db1.to_csv(qc+'final_db1.csv')

#%%
#Mapping BDA to POS Retailers/Channels 
coeff_db_map = pd.read_excel(path+'Mapping Files/'+'Hyperion DB Channels.xlsx','Hyperion DB Channels')
dataf1 = final_db1.merge(coeff_db_map, on = ['Geography_Name', 'Geography_Level', 'model_source'], how = 'left')
dataf2 = dataf1[dataf1['IRI Channels'].isnull() == False]
dataf2.to_csv(qc+'dataf2.csv')

#%%
dataf2_w_iriprod = dataf2[dataf2['iriprod'].isnull()==False].reset_index(drop=True)
dataf2_wo_iriprod = dataf2[dataf2['iriprod'].isnull()==True].reset_index(drop=True) 

#%%
#Custom Aggregate Keys Mapping
df_CL = pd.read_excel(cust_path+'CustAggs_FY21Q1 - CW.xlsx', 'SKUs_to_Aggregate')
df_CL = df_CL[['Catcode','Prodlvl','Prodkey','Custprod','IRI_Product_Key','Product_Name']]

df_DL = pd.read_excel(cust_path+'CustAggs_FY21Q1 - DL.xlsx', 'SKUs_to_Aggregate')
df_DL = df_DL[['Catcode','Prodlvl','Prodkey','Custprod','IRI_Product_Key','Product_Name']]

df_SP = pd.read_excel(cust_path+'CustAggs_FY21Q1 - SP.xlsx', 'SKUs_to_Aggregate')
df_SP = df_SP[['Catcode','Prodlvl','Prodkey','Custprod','IRI_Product_Key','Product_Name']]

df_cust = df_CL.append([df_DL, df_SP])

cust_agg_keys = df_cust[(df_cust['Prodlvl']=='S') & (pd.isnull(df_cust['Prodkey'])==False)]
cust_agg_keys_w_cust_cnt = cust_agg_keys.groupby(['Custprod'])['Custprod'].count().reset_index(name="count")
cust_agg_keys_w_cust_cnt.to_csv(qc+'cust_agg_keys_w_cust_cnt.csv')

#%%
cust_agg_keys1 = cust_agg_keys.merge(cust_agg_keys_w_cust_cnt, on = ['Custprod'], how = 'left')
cust_agg_keys1.to_csv(qc+'cust_agg_keys1.csv')

#%%
dataf3_1 = dataf2_wo_iriprod.merge(cust_agg_keys1, left_on=['prodkey'], right_on=['Custprod'], how = 'left')
dataf3_1['Base_Statcase_Volume2'] = dataf3_1.apply(lambda x: x['Base_Statcase_Volume'] if pd.isnull(x['Custprod'])==True
                      else x['Base_Statcase_Volume']/x['count'], axis=1)
dataf3_1.drop(['iriprod'],axis=1,inplace=True)
dataf3_1.rename(columns={'IRI_Product_Key':'iriprod'},inplace=True)
dataf3_1.to_csv(qc+'dataf3_1.csv')

#%%
dataf3 = dataf3_1.append([dataf2_w_iriprod])
dataf3['Base_Statcase_Volume'] = dataf3.apply(lambda x: x['Base_Statcase_Volume'] if pd.isnull(x['count'])==True
                      else x['Base_Statcase_Volume2'], axis=1)
dataf3.to_csv(qc+'dataf3.csv')

dataf4 = dataf3[['model_source', 'Geography_Level', 'Geography_Name', 'IRI Channels', 'Model_Period_End',
    'Product_Level','catlib','Product_Name_Modeled','Product_Name_Current','Product_Name','prodkey',
    'CLOROX VS COMP','iriprod','Base_Price_Elasticity', 'Promo_Price_Elasticity','Base_Statcase_Volume']]
dataf5 = dataf4[dataf4['Base_Statcase_Volume']>0]
dataf5.to_csv(qc+'dataf5.csv')

#%%
def roll_a(x):
    d = {} 
    d['Base_Statcase_Volume'] = x['Base_Statcase_Volume'].sum()
    d['Promo_Price_Elasticity'] = np.average(x['Promo_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    d['Base_Price_Elasticity'] = np.average(x['Base_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    return pd.Series(d, index=['Promo_Price_Elasticity','Base_Price_Elasticity','Base_Statcase_Volume'])

#%%
dataf5['Product_Name_Current'] = dataf5['Product_Name_Current'].str.upper()
Pdt = dataf5[['IRI Channels','Product_Name_Current','iriprod']]
Pdt = Pdt.rename(columns={'IRI Channels':'Geography'})
Pdt = Pdt.drop_duplicates()

#%%
CoefDb_All = dataf5.groupby(['iriprod','IRI Channels', 'CLOROX VS COMP', 'Model_Period_End', 'catlib']).apply(roll_a).reset_index()
CoefDb_All = CoefDb_All.rename(columns={'IRI Channels':'Geography'})
CoefDb_All['Geography'] = CoefDb_All['Geography'].str.strip().str.upper()
CoefDb_All.to_csv(qc+'CoefDb_All.csv')

#%%
#Start of manipulation to determine BDA lite for HC. Check if there is any new catlib for HC before proceeding.
HC = CoefDb_All[CoefDb_All['catlib'].isin(['CW','LP','S1','S2','S3','SP','SB','SD','SS','SW','T2','TA','TM','TW','TX','W2','UR'])]
HC.to_csv(qc+'HC.csv')

#%%
HC_New = HC[HC['Model_Period_End']>='2021-03-28']
HC_Old = HC[HC['Model_Period_End'] < '2021-03-28']

#%%
HC_pivot = pd.pivot_table(HC_Old, values=['Base_Statcase_Volume'], index=['catlib', 'Model_Period_End','CLOROX VS COMP'],
                    columns =['Geography'], aggfunc = {'Base_Statcase_Volume' : sum})

#%%
HC_pivot.columns = HC_pivot.columns.droplevel(0)
HC_1 = HC_pivot.reset_index().rename_axis(None, axis=1)
HC_1.to_csv(qc+'HC_1.csv')

#%%
#Deleting catlib and model period for which Wal and TUS is absent - BDA Lite
HC_1.dropna(subset=['WALMART CORP-RMA - WALMART','TOTAL US - FOOD'],inplace=True)
HC_2 = HC_1.melt(['catlib','Model_Period_End','CLOROX VS COMP'], var_name ='Geography')
HC_2.to_csv(qc+'HC_2.csv')

#%%
HC_2 = HC_2[['catlib','Model_Period_End','CLOROX VS COMP','Geography']].drop_duplicates()
HC_final = HC_2.merge(HC,on=['catlib','Model_Period_End','CLOROX VS COMP','Geography'],how='left')
HC_final.to_csv(qc+'HC_final_check.csv')

#%%
# Dropping all such rows for which a catlib is not modelled for a particular retailer in a period. Came as as result of pivot.
HC_final.dropna(subset=['Base_Statcase_Volume'],inplace=True)

#%%
HC_final = HC_final.append([HC_New])

#%%
#Time period mapping
HC_final['Time Period'] = HC_final['Model_Period_End'].map(time_map.set_index('Model_Period_End')['modeling_period'])
HC_final['Geography'] = HC_final['Geography'].str.upper().map(geog_map.set_index('Geography_Name')['Geography'])

#%%
#New Product Key for Brand, Subbrand Mapping
HC_final['new'] = HC_final['iriprod'].str.split(':')
HC_final['new_split_irip'] = HC_final['new'].apply(lambda x : x[:-3])
HC_final['New iriprod'] = HC_final['new_split_irip'].str.join(':')
HC_final.drop(['new','new_split_irip'],axis=1,inplace=True)
HC_final.to_csv(qc+'HC_final.csv')

#End of manipulation to determine BDA lite for HC

#%%
# For Uncured view - ranking based on subbrand
# =============================================================================
CoefDb_All1 = HC_final.copy()

# Need to reset index so that Ranks can be assigned later (Avoid duplication of index for ranking)
CoefDb_All1.reset_index(drop=True, inplace=True)
CoefDb_All1.to_csv(qc+'CoefDb_All_check1.csv')

#%%
CoefDb_subb = CoefDb_All1.merge(pos_hc, left_on = ['Geography','New iriprod'], right_on = ['Geography','Product Key'], how='left')

#Imp step - Check blank Subbrands and do the qc
CoefDb_subb.to_csv(qc+'CoefDb_subb.csv')

CoefDb_subb = CoefDb_subb[CoefDb_subb['Sub_Brand'].notnull()]
CoefDb_subb.to_csv(qc+'CoefDb_subb1.csv')

#%%
# Ranking based on time period 
# Ranking should not include Geography. It'll mess up dashboard view
year_hc = CoefDb_subb[['Sub_Brand','Time Period']].drop_duplicates().reset_index(drop=True)
year_hc['Year'] = year_hc['Time Period'].apply(lambda x : x[2:4]).astype('int')
year_hc['Quarter'] = year_hc['Time Period'].apply(lambda x : x[5:6]).astype('int')
year_hc['rank'] = year_hc.sort_values(['Sub_Brand','Year','Quarter'], ascending = False).groupby(['Sub_Brand']).cumcount()+1
year_hc = year_hc.sort_values(['Sub_Brand','Year','Quarter']).reset_index(drop=True)

# QC evidence
year_hc.to_csv(qc+'year_hc.csv')

bda_raw_hc_all = pd.merge(CoefDb_subb, year_hc, on = ['Sub_Brand','Time Period'], how = 'left')
bda_raw_hc_all.to_csv(qc+'bda_raw_hc_all.csv')

#%%
bda_raw_hc_all['Flag_0.3'] = bda_raw_hc_all['Base_Price_Elasticity'].apply(lambda x: 1 if x==-0.3 else 0)
bda_raw_hc_all['Flag_5'] = bda_raw_hc_all['Base_Price_Elasticity'].apply(lambda x: 1 if x==-5.0 else 0)
bda_raw_hc_all.to_csv(qc+'bda_raw_hc_all1.csv')

#%%
#Differentiating the main file file for the 2 rank periods for z_scores.
D_f = bda_raw_hc_all.copy()
four = D_f[D_f['rank']<=4]
g_four = D_f[D_f['rank']>=5]

#%%
g_four['z_BPE']  = np.nan
g_four['z_PPE']  = np.nan
g_four['z_BSCV'] = np.nan

#%%
four['z_BPE'] = four.groupby(['iriprod','Geography']).Base_Price_Elasticity.transform(lambda x : zscore(x))
four['z_PPE'] = four.groupby(['iriprod','Geography']).Promo_Price_Elasticity.transform(lambda x : zscore(x))
four['z_BSCV'] = four.groupby(['iriprod','Geography']).Base_Statcase_Volume.transform(lambda x : zscore(x))

#%%
four['z_BPE'] = four['z_BPE'].replace(np.nan,0)
four['z_PPE'] = four['z_PPE'].replace(np.nan,0)
four['z_BSCV'] = four['z_BSCV'].replace(np.nan,0)

#%%
Result = four.append(g_four)
Result.to_csv(qc+'Result.csv')

# The result file will be the main feed file for Product and retailer level 

#%%
O_a = Result.copy()
O_a = O_a[ (O_a['Base_Price_Elasticity']==-5) | (O_a['Base_Price_Elasticity']==-0.3) ]
O_a.to_csv(qc+'Proxy_el_Pid.csv')

#%%
two_5 = Result.copy()
two_5 = two_5[(two_5['z_BPE']>=2.5) | (two_5['z_BPE']<=-2.5)]
two_5.to_csv(qc+'exceed_std_pid.csv')

#%%
def sb_elas(x):
    d = {}
    d['BPE_by_channel'] =np.average(x['Base_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    d['PPE_by_channel'] =np.average(x['Promo_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    d['Base_Statcase_Volume'] = x['Base_Statcase_Volume'].sum() 
    d['Flag_0.3'] = x['Flag_0.3'].sum()
    d['Flag_5']   = x['Flag_5'].sum()  
    return pd.Series(d, index=['BPE_by_channel','PPE_by_channel','Base_Statcase_Volume','Flag_0.3','Flag_5'])

#%%
sb_feed_channel = bda_raw_hc_all.groupby(['Division','Geography','Time Period','rank','SBU','Brand','Sub_Brand']).apply(sb_elas).reset_index()
sb_feed_channel.to_csv(qc+'sb_feed_channel.csv')

#%%
sb_feed_channel['Flag_0.3'] = sb_feed_channel['Flag_0.3'].apply(lambda x: 0 if x==0 else 1)
sb_feed_channel['Flag_5'] = sb_feed_channel['Flag_5'].apply(lambda x: 0 if x==0 else 1)
sb_feed_channel.to_csv(qc+'sb_feed_channel.csv')

#%%
D_f_sub = sb_feed_channel.copy()
four_sub = D_f_sub[D_f_sub['rank']<=4]
g_four_sub = D_f_sub[D_f_sub['rank']>=5]

#%%
g_four_sub['z_BPE']  = np.nan
g_four_sub['z_PPE']  = np.nan
g_four_sub['z_BSCV'] = np.nan

#%%
four_sub['z_BPE'] = four_sub.groupby(['Sub_Brand','Geography']).BPE_by_channel.transform(lambda x : zscore(x))
four_sub['z_PPE'] = four_sub.groupby(['Sub_Brand','Geography']).PPE_by_channel.transform(lambda x : zscore(x))
four_sub['z_BSCV'] = four_sub.groupby(['Sub_Brand','Geography']).Base_Statcase_Volume.transform(lambda x : zscore(x))

#%%
four_sub['z_BPE'] = four_sub['z_BPE'].replace(np.nan,0)
four_sub['z_PPE'] = four_sub['z_PPE'].replace(np.nan,0)
four_sub['z_BSCV'] = four_sub['z_BSCV'].replace(np.nan,0)

#%%
Result_sub = four_sub.append(g_four_sub)
Result_sub.to_csv(qc+'Result_sub.csv')

# the result_sub file is the main file at subbrand and retailer level

#%%
SB_lvl_qc = Result_sub[(Result_sub['z_BPE'] >=2.5) | (Result_sub['z_BPE'] <=-2.5)]
SB_lvl_qc2 = Result_sub[(Result_sub['BPE_by_channel'] == -5) | (Result_sub['BPE_by_channel'] == -0.3)]

#%%
SB_lvl_qc.to_csv(qc+'exceed_std_sb.csv')
SB_lvl_qc2.to_csv(qc+'Proxy_el_sb.csv')

#%%
# The following code for Outlier Analysis :
File_1 = ['Result.csv','Proxy_el_Pid.csv','exceed_std_pid.csv']
File_2 = ['Result_sub.csv','exceed_std_sb.csv','Proxy_el_sb.csv']
File_New = []

#%%
def R_d(file):
   for file_name in file:
        file = pd.read_csv(qc+file_name)
        if file.shape[0] != 0:
            print(file_name)
            File_New.append(file_name)

#%%
R_d(File_1)
R_d(File_2)
print(File_New)

#%%
sb_feed_totalUS_rank = Result_sub[['Division','SBU','Brand','Sub_Brand','Time Period','rank']].drop_duplicates() 
sb_feed_BPE_totalUS = Result_sub.groupby(['Division','SBU','Brand','Sub_Brand','Time Period']).apply(lambda x: np.average(x['BPE_by_channel'], weights=x['Base_Statcase_Volume'])).reset_index().rename(columns = {0:'BPE_TotalUS'})
sb_feed_PPE_totalUS = Result_sub.groupby(['Division','SBU','Brand','Sub_Brand','Time Period']).apply(lambda x: np.average(x['PPE_by_channel'], weights=x['Base_Statcase_Volume'])).reset_index().rename(columns = {0:'PPE_TotalUS'})

#%%
# Merged with rank level information.    
sb_feed_BPE_totalUS = sb_feed_totalUS_rank.merge(sb_feed_BPE_totalUS, on = ['Division','SBU', 'Brand', 'Sub_Brand',
                                                                            'Time Period'], how = 'left')    
sb_feed_PPE_totalUS = sb_feed_totalUS_rank.merge(sb_feed_PPE_totalUS, on = ['Division','SBU', 'Brand', 'Sub_Brand',
                                                                            'Time Period'], how = 'left')   

#%%
sb_bda_BPE = pd.merge(Result_sub,sb_feed_BPE_totalUS, on = ['Division','SBU', 'Brand', 'Sub_Brand','Time Period','rank'], how = 'left' )
sb_bda = pd.merge(sb_bda_BPE,sb_feed_PPE_totalUS, on = ['Division','SBU', 'Brand', 'Sub_Brand','Time Period','rank'], how = 'left' )
sb_bda.to_csv(output+'trended_bda_FY22Q3_HC_uncured+3cured.csv')

# Uncured view manipulation complete - ranking based on subbrand
# =============================================================================

#%%
# Cured view - latest period
# =============================================================================
#Check if there is any new catlib for HC before proceeding.
CoefDb_All = HC_final.copy()

# Need to reset index so that Ranks can be assigned later (Avoid duplication of index for ranking)
CoefDb_All.reset_index(drop=True, inplace=True)
CoefDb_All.to_csv(qc+'CoefDb_All_check.csv')

#%%
# For Cured view - ranking based on geo and ret
#Select Latest 4 periods for all retailers and product keys
CoefDb_All['date'] = pd.to_datetime(CoefDb_All['Model_Period_End'],format='%Y-%m-%d')
CoefDb_All['year'] = pd.DatetimeIndex(CoefDb_All['date']).year
CoefDb_All['month'] = pd.DatetimeIndex(CoefDb_All['date']).month
CoefDb_All['Rank'] = CoefDb_All.sort_values(['Geography','iriprod','CLOROX VS COMP','year','month'], ascending = False).groupby(['Geography','iriprod', 'CLOROX VS COMP']).cumcount()+1
CoefDb_All.to_csv(qc+'CoefDb_All_ranked.csv')

#%%
CoefDb_All_Cl1 = CoefDb_All[CoefDb_All['Rank']<=4]
CoefDb_All_Cl1.to_csv(qc+'CoefDb_latest_4.csv')

#%%
CoefDb_All_Cl2 = CoefDb_All_Cl1.groupby(['iriprod','Geography']).apply(roll_a).reset_index()
CoefDb_All_F = CoefDb_All_Cl2.copy()
CoefDb_All_F.to_csv(qc+'CoefDb_All_F.csv')

#%%
#Latest 4 Period Aggregated
#1. Left Join 
POS_CoefDb_All = iri_df3.merge(CoefDb_All_F,left_on=['Product Key','Geography'],right_on=['iriprod','Geography'], how='left')
POS_CoefDb_All.to_csv(qc+'POS_CoefDb_All.csv')

#%%
#Filtering out mapped POS+BDA after Key-Mapping
POS_CoefDb_All_mapped = POS_CoefDb_All.loc[POS_CoefDb_All['iriprod'].notnull()]

# 1st df to be appended
POS_CoefDb_All_mapped.to_csv(qc+'POS_CoefDb_All_mapped.csv')

#%%
#Filtering out unmapped POS+BDA after Key-Mapping
POS_CoefDb_All_unmapped = POS_CoefDb_All.loc[POS_CoefDb_All['iriprod'].isnull()]
POS_CoefDb_All_unmapped.to_csv(qc+'POS_CoefDb_All_unmapped.csv')

#%%
# > $5000 Baseline Dollar Sales
POS_CoefDb_All_unmapped = POS_CoefDb_All_unmapped[POS_CoefDb_All_unmapped['Dollar Sales'] >= 5000].reset_index(drop=True)
POS_CoefDb_All_unmapped.to_csv(qc+'POS_CoefDb_All_unmapped1.csv')

#%%
iri_df4 = POS_CoefDb_All_unmapped.drop(['iriprod', 'Promo_Price_Elasticity','Base_Price_Elasticity', 'Base_Statcase_Volume'], axis = 1)

#New Product Key = Product Key - 1
iri_df4['new_split_pk'] = iri_df4['new'].apply(lambda x : x[:-1])
iri_df4['New Product Key'] = iri_df4['new_split_pk'].str.join(':')
iri_df4.drop(['new','new_split_pk'],axis=1,inplace=True)
iri_df4.to_csv(qc+'iri_df4.csv')

#%%
#New iriprod = iriprod - 1
CoefDb_All_F['new_split'] = CoefDb_All_F['iriprod'].str.split(':')
CoefDb_All_F['new_split_iri'] = CoefDb_All_F['new_split'].apply(lambda x : x[:-1])
CoefDb_All_F['New iriprod'] = CoefDb_All_F['new_split_iri'].str.join(':')
CoefDb_All_F.drop(['new_split','new_split_iri'],axis=1,inplace=True)

#%%
def proxy_roll_a(x):
    d = {} 
    d['Base_Statcase_Volume'] = x['Base_Statcase_Volume'].mean()
    d['Promo_Price_Elasticity'] = np.average(x['Promo_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    d['Base_Price_Elasticity'] = np.average(x['Base_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    return pd.Series(d, index=['Promo_Price_Elasticity','Base_Price_Elasticity','Base_Statcase_Volume'])

#%%
#bda aggregation after Key-1
CoefDb_All_F_Agg = CoefDb_All_F.groupby(['New iriprod','Geography']).apply(proxy_roll_a).reset_index()
CoefDb_All_F_Agg.to_csv(qc+'CoefDb_All_F_new_iri_prod.csv')

#%%
#Latest 4 Period Aggregated
#1. Left Join unmapped POS with BDA at Key - 1
# 2nd df to be appended
POS_CoefDb_All_nw_pdt_key = iri_df4.merge(CoefDb_All_F_Agg, left_on=['New Product Key','Geography'],right_on=['New iriprod','Geography'], how='left')
POS_CoefDb_All_nw_pdt_key.to_csv(qc+'POS+Elasticity_nw_pdt_key.csv')

#%%
#Rule 1 completed - Appending Key - Mapped data with Key - 1 mapped
POS_CoefDb_All_updated = POS_CoefDb_All_mapped.append(POS_CoefDb_All_nw_pdt_key)
POS_CoefDb_All_updated.to_csv(qc+'POS+Elasticity_RULE1.csv')

#%%
#Filtering out mapped POS+BDA after Key and Key-1 Mapping
POS_CoefDb_All_updated_mapped = POS_CoefDb_All_updated.loc[POS_CoefDb_All_updated['iriprod'].notnull() | POS_CoefDb_All_updated['New iriprod'].notnull()] 
POS_CoefDb_All_updated_mapped.to_csv(qc+'POS_CoefDb_All_updated_mapped.csv')

#%%
#Filtering out unmapped POS+BDA after Key and Key-1 Mapping
POS_CoefDb_All_updated_unmapped = POS_CoefDb_All_updated.loc[pd.isnull(POS_CoefDb_All_updated['iriprod']) & pd.isnull(POS_CoefDb_All_updated['New iriprod'])]
POS_CoefDb_All_updated_unmapped.to_csv(qc+'POS+Elasticity_updated_unmapped.csv')

#%%
#Filtering out unmapped POS+BDA after Key and Key-1 Mapping having only Food and Mass Retailers
POS_CoefDb_All_updated_unmapped_FOMA = POS_CoefDb_All_updated_unmapped[~POS_CoefDb_All_updated_unmapped['Geography'].isin(['TOTAL U.S. GROCERY', 
'Total US - Multi Outlet', 'Total Mass Aggregate', 'Total US - Drug', 'Petco Corp-RMA - Pet', "TOTAL U.S. SAMS CLUB", "BJ's Corp-RMA - Club"])]

#Filtering out unmapped POS+BDA after Key and Key-1 Mapping having all Retailers/Channels except Food and Mass
POS_CoefDb_All_updated_unmapped_TCP = POS_CoefDb_All_updated_unmapped[POS_CoefDb_All_updated_unmapped['Geography'].isin(['TOTAL U.S. GROCERY', 
'Total US - Multi Outlet', 'Total Mass Aggregate', 'Total US - Drug', 'Petco Corp-RMA - Pet', "TOTAL U.S. SAMS CLUB", "BJ's Corp-RMA - Club"])]

#%%
#POS data for unmapped after Key and Key-1 Mapping
iri_df6 = POS_CoefDb_All_updated_unmapped_FOMA.drop(['iriprod','New iriprod','Promo_Price_Elasticity','Base_Price_Elasticity', 'Base_Statcase_Volume'], axis = 1)
iri_df6.rename(columns = {'Geography':'Geography_unmapped'},inplace=True)
iri_df6.to_csv(qc+'iri_df6.csv')
Geography_unmapped = iri_df6['Geography_unmapped'].unique()
print(Geography_unmapped)

#%%
#Reading the geography proxy file. This file needs to be updated everytime there is a new unmapped Geography in iri_df4  
geo_pxy  = pd.read_csv(path +'Mapping Files/'+'Geo Proxy Mapping.csv')

#%%
# Iterating through the list of unmapped retailers
POS_CoefDb_RULE2_0 = pd.DataFrame()
for geo in Geography_unmapped:
    print(geo)
    iri_df6_Geo = iri_df6[iri_df6['Geography_unmapped'] == geo ] 
    iri_df6_Geo =  iri_df6_Geo.merge(geo_pxy, on = ['Geography_unmapped'], how = 'inner')
    POS_CoefDb_RULE2_0 = POS_CoefDb_RULE2_0.append([iri_df6_Geo.merge(CoefDb_All_F_Agg, left_on = ['New Product Key','Geography_Proxy'], right_on = 
    ['New iriprod','Geography'], how = 'left')])

POS_CoefDb_RULE2_0['Geography Proxy'] = 'Yes'
POS_CoefDb_RULE2_0.to_csv(qc+'POS_CoefDb_RULE2_0.csv')

#%%
#Filtering out only the BDA file information from the appended data
CoefDb_RULE2 = POS_CoefDb_RULE2_0[['New iriprod','Geography_unmapped','Promo_Price_Elasticity','Base_Price_Elasticity', 
                                   'Base_Statcase_Volume']]

#Duplicates are formed in the BDA file as each retailer within a channel gets mapped to multiple retailers within a channel
#Duplicates removed and BDA rolled once again 
CoefDb_RULE2 = CoefDb_RULE2.drop_duplicates()
CoefDb_RULE2_rolled = CoefDb_RULE2.groupby(['New iriprod','Geography_unmapped']).apply(proxy_roll_a).reset_index() 

#%%
#Dropping Geo, Geo keys and BDA data from the appended dataframe
POS_CoefDb_RULE2_1 = POS_CoefDb_RULE2_0.drop(['Geography','Geography_Proxy','Promo_Price_Elasticity',
                                              'Base_Price_Elasticity', 'Base_Statcase_Volume'],axis=1)

#Each retailer does not get mapped to all retailers within a channel. Dropping all such rows.
POS_CoefDb_RULE2_1.dropna(subset = ["New iriprod"], inplace=True)

#Duplicates on POS data fromed due to same reason as above. Those being dropped.
POS_CoefDb_RULE2_1 = POS_CoefDb_RULE2_1.drop_duplicates()

#%%
#Left Join POS after duplicate removal with rolled up BDA. Completion of Rule 2
# 3rd df to be appended
POS_CoefDb_RULE2 = POS_CoefDb_RULE2_1.merge(CoefDb_RULE2_rolled, on=['New iriprod','Geography_unmapped'],how='left')
POS_CoefDb_RULE2.rename(columns = {'Geography_unmapped':'Geography'},inplace=True)

#%%
#Dropping Geo, Geo keys and BDA data from the appended dataframe
POS_CoefDb_RULE2_1_0 = POS_CoefDb_RULE2_0.drop(['Geography','Geography_Proxy','Promo_Price_Elasticity',
                                              'Base_Price_Elasticity', 'Base_Statcase_Volume'],axis=1)

#Each retailer does not get mapped to all retailers within a channel. Appending all such rows.
POS_CoefDb_RULE2_1_0 = POS_CoefDb_RULE2_1_0[POS_CoefDb_RULE2_1_0['New iriprod'].isna()].reset_index(drop=True)

#Duplicates on POS data fromed due to same reason as above. Those being dropped.
POS_CoefDb_RULE2_1_0 = POS_CoefDb_RULE2_1_0.drop_duplicates()
POS_CoefDb_RULE2_1_0.rename(columns = {'Geography_unmapped':'Geography'},inplace=True)

#%%
#Appending Unmapped Food data with Rule 2 data
POS_CoefDb_RULE2 = POS_CoefDb_RULE2.append([POS_CoefDb_RULE2_1_0])

#%%
POS_CoefDb_RULE2['is_duplicate'] = POS_CoefDb_RULE2[['Geography','Product Key','Product']].duplicated()
POS_CoefDb_RULE2_nd = POS_CoefDb_RULE2[POS_CoefDb_RULE2['is_duplicate']== False]
POS_CoefDb_RULE2_d = POS_CoefDb_RULE2[POS_CoefDb_RULE2['is_duplicate']== True] 
POS_CoefDb_RULE2_d = POS_CoefDb_RULE2_d[POS_CoefDb_RULE2_d['New iriprod'].notna()]
POS_CoefDb_RULE2 = POS_CoefDb_RULE2_nd.append([POS_CoefDb_RULE2_d])
POS_CoefDb_RULE2.to_csv(qc+'POS_CoefDb_RULE2.csv')

#%%
#Appending Unmapped Total, Club data with Rule 2 data
POS_CoefDb_RULE2_All = POS_CoefDb_All_updated_unmapped_TCP.append([POS_CoefDb_RULE2])

#%%
#Appending mapped Key and Key-1 data with Rule 2 and Unmapped Total, Club data. Completion of Rule 1+2
POS_CoefDb_RULE12 = POS_CoefDb_All_updated_mapped.append([POS_CoefDb_RULE2_All])

#%%
#Creating MAP STAT and MAP TYPE columns
POS_CoefDb_RULE12['MAP STAT'] = np.where(POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull(), 'UNMAP', 'MAP')

conditions = [(POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull()),
              
(~POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull() & 
POS_CoefDb_RULE12['New Product Key'].isnull() &
POS_CoefDb_RULE12['Geography Proxy'].isnull()),

(~POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull() & 
~POS_CoefDb_RULE12['New Product Key'].isnull() &
POS_CoefDb_RULE12['Geography Proxy'].isnull()),

(~POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull() & 
~POS_CoefDb_RULE12['New Product Key'].isnull() &
~POS_CoefDb_RULE12['Geography Proxy'].isnull())]

choices = ['UNMAP', 'GEO-KEY MAP', 'GEO-SIZE MAP', 'GEO PROXY-SIZE MAP']

POS_CoefDb_RULE12['MAP TYPE'] = np.select(conditions, choices, default=np.nan)
POS_CoefDb_RULE12.to_csv(qc+'POS+Elasticity_RULE1+2_HC.csv')

#%%
#New Product Key for Brand, Subbrand Mapping
POS_CoefDb_RULE12['new'] = POS_CoefDb_RULE12['Product Key'].str.split(':')
POS_CoefDb_RULE12['new_split_pk'] = POS_CoefDb_RULE12['new'].apply(lambda x : x[:-3])
POS_CoefDb_RULE12['New Product Key'] = POS_CoefDb_RULE12['new_split_pk'].str.join(':')
POS_CoefDb_RULE12.drop(['new','new_split_pk'],axis=1,inplace=True)

#%%
POS_CoefDb_RULE12 = POS_CoefDb_RULE12.merge(pos_hc, left_on=['New Product Key','Geography'], right_on = ['Product Key','Geography'], how='left')
POS_CoefDb_RULE12.to_csv(qc+'POS+Elasticity_RULE1+2_with_subbrand.csv')

#%%
hc_clx = POS_CoefDb_RULE12[POS_CoefDb_RULE12['Clx_Comp'].isin(['Clorox'])
                    & ~ POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull()]

#%%
def sb_elas_latest(x):
    d = {}
    d['BPE_by_channel_latest'] =np.average(x['Base_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    d['PPE_by_channel_latest'] =np.average(x['Promo_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    d['Base_Statcase_Volume_latest'] = x['Base_Statcase_Volume'].sum() 
    return pd.Series(d, index=['BPE_by_channel_latest','PPE_by_channel_latest','Base_Statcase_Volume_latest'])

#%%
sb_bda_latest = sb_bda[sb_bda['rank']==1]
sb_bda_latest = sb_bda_latest[['Division','Time Period','rank','SBU','Brand','Sub_Brand']].drop_duplicates() 
hc_clx = hc_clx.merge(sb_bda_latest, on=['Division','SBU','Brand','Sub_Brand'], how ='left')
hc_clx.to_csv(qc+'hc_clx.csv')

#%%
sb_feed_channel_latest = hc_clx.groupby(['Division','Geography','Time Period','rank','SBU','Brand','Sub_Brand']).apply(sb_elas_latest).reset_index()
sb_feed_channel_latest.to_csv(qc+'sb_feed_channel_latest.csv')

#%%
# Jishnu - Rank has been removed from groupby for totalUS. This is done BPE_totalUS has same elasticities for a time period.    
sb_feed_totalUS_rank_latest = sb_feed_channel_latest[['Division','SBU','Brand','Sub_Brand','Time Period','rank']].drop_duplicates() 
    
#%%
sb_feed_BPE_totalUS_latest = sb_feed_channel_latest.groupby(['Division','SBU','Brand','Sub_Brand','Time Period']).apply(lambda x: np.average(x['BPE_by_channel_latest'], weights=x['Base_Statcase_Volume_latest'])).reset_index().rename(columns = {0:'BPE_TotalUS_latest'})
sb_feed_PPE_totalUS_latest = sb_feed_channel_latest.groupby(['Division','SBU','Brand','Sub_Brand','Time Period']).apply(lambda x: np.average(x['PPE_by_channel_latest'], weights=x['Base_Statcase_Volume_latest'])).reset_index().rename(columns = {0:'PPE_TotalUS_latest'})

#%%
# Merged with rank level information.    
sb_feed_BPE_totalUS_latest = sb_feed_totalUS_rank_latest.merge(sb_feed_BPE_totalUS_latest, on = ['Division','SBU', 'Brand', 'Sub_Brand',
                                                                            'Time Period'], how = 'left')    
sb_feed_PPE_totalUS_latest = sb_feed_totalUS_rank_latest.merge(sb_feed_PPE_totalUS_latest, on = ['Division','SBU', 'Brand', 'Sub_Brand',
                                                                            'Time Period'], how = 'left')   
    
#%%
sb_bda_BPE_latest = pd.merge(sb_feed_channel_latest , sb_feed_BPE_totalUS_latest, on = ['Division','SBU',
                                        'Brand', 'Sub_Brand','Time Period','rank'], how = 'left' )
sb_bda_latest = pd.merge(sb_bda_BPE_latest, sb_feed_PPE_totalUS_latest, on = ['Division','SBU', 
                                        'Brand', 'Sub_Brand','Time Period','rank'], how = 'left' )
sb_bda_latest.to_csv(output+'trended_bda_FY22Q3_HC_latest_cured.csv')

# Uncured view latest period - complete
# =============================================================================

#%%
sb_bda_all = sb_bda.append(sb_bda_latest, ignore_index = True)
sb_bda_all.to_csv(qc+'sb_bda_all.csv')

#%%
sb_bda_all_3 = sb_bda_all[sb_bda_all['rank'].isin([4,3,2])]
sb_bpe_all_3_sum = sb_bda_all_3.groupby(['Division','Geography','SBU','Brand','Sub_Brand']).apply(lambda x: np.sum(x['BPE_by_channel'])).reset_index().rename(columns = {0:'BPE_by_channel_sum'})
sb_ppe_all_3_sum = sb_bda_all_3.groupby(['Division','Geography','SBU','Brand','Sub_Brand']).apply(lambda x: np.sum(x['PPE_by_channel'])).reset_index().rename(columns = {0:'PPE_by_channel_sum'})

#%%
sb_bpe_tus_3_sum = sb_bda_all_3.groupby(['Division','Geography','SBU','Brand','Sub_Brand']).apply(lambda x: np.sum(x['BPE_TotalUS'])).reset_index().rename(columns = {0:'BPE_TotalUS_sum'})
sb_ppe_tus_3_sum = sb_bda_all_3.groupby(['Division','Geography','SBU','Brand','Sub_Brand']).apply(lambda x: np.sum(x['PPE_TotalUS'])).reset_index().rename(columns = {0:'PPE_TotalUS_sum'})

#%%
sb_bda_all_3 = sb_bda_all_3.merge(sb_bpe_all_3_sum, on = ['Division','Geography','SBU','Brand','Sub_Brand'],how='left')
sb_bda_all_3 = sb_bda_all_3.merge(sb_ppe_all_3_sum, on = ['Division','Geography','SBU','Brand','Sub_Brand'],how='left')

#%%
sb_bda_all_3 = sb_bda_all_3.merge(sb_bpe_tus_3_sum, on = ['Division','Geography','SBU','Brand','Sub_Brand'],how='left')
sb_bda_all_3 = sb_bda_all_3.merge(sb_ppe_tus_3_sum, on = ['Division','Geography','SBU','Brand','Sub_Brand'],how='left')

#%%
sb_bda_all_3 = sb_bda_all_3[['Division', 'Geography','SBU', 'Brand','Sub_Brand','BPE_by_channel_sum','PPE_by_channel_sum',
                             'BPE_TotalUS_sum', 'PPE_TotalUS_sum']].drop_duplicates() 
sb_bda_all_dup = sb_bda_all.merge(sb_bda_all_3, on = ['Division','Geography','SBU','Brand','Sub_Brand'], how='left')

#%%
sb_bda_all_1 = sb_bda_all_dup[sb_bda_all_dup['rank'].isin([2,3,4]) | (sb_bda_all_dup['rank'].isin([1]) & sb_bda_all_dup['BPE_by_channel'].isnull())]
sb_bda_all_1.to_csv(qc+'sb_bda_all_1.csv')

#%%
sb_bda_all_1_unq_rnk = sb_bda_all_1.groupby(['Division','Geography',
'SBU','Brand','Sub_Brand'], sort=False)['rank'].nunique().reset_index().rename(columns={'rank':'unique_rnk'})
sb_bda_all_1_unq_rnk.to_csv(qc+'sb_bda_all_1_unq_rnk.csv')

#%%
sb_bda_all_1 = sb_bda_all_1.merge(sb_bda_all_1_unq_rnk, on=['Division','Geography','SBU','Brand','Sub_Brand'], how ='left')
sb_bda_all_1.to_csv(qc+'sb_bda_all_1_check.csv')

#%%
sb_bda_all_1['BPE_by_channel1'] = sb_bda_all_1.apply(lambda x: x['unique_rnk']*x['BPE_by_channel_latest'] - x['BPE_by_channel_sum']
            if pd.isna(x['BPE_by_channel']) else x['BPE_by_channel'], axis=1)
sb_bda_all_1['PPE_by_channel1'] = sb_bda_all_1.apply(lambda x: x['unique_rnk']*x['PPE_by_channel_latest'] - x['PPE_by_channel_sum']
            if pd.isna(x['PPE_by_channel']) else x['PPE_by_channel'], axis=1)
sb_bda_all_1['BPE_TotalUS1'] = sb_bda_all_1.apply(lambda x: x['unique_rnk']*x['BPE_TotalUS_latest'] - x['BPE_TotalUS_sum']
            if pd.isna(x['BPE_TotalUS']) else x['BPE_TotalUS'], axis=1)
sb_bda_all_1['PPE_TotalUS1'] = sb_bda_all_1.apply(lambda x: x['unique_rnk']*x['PPE_TotalUS_latest'] - x['PPE_TotalUS_sum']
            if pd.isna(x['PPE_TotalUS']) else x['PPE_TotalUS'], axis=1)
sb_bda_all_1.to_csv(qc+'sb_bda_all_1_final.csv')

#%%
sb_bda_all_gr_4 = sb_bda_all[sb_bda_all['rank']>4]
sb_bda_all_new = sb_bda_all[sb_bda_all['rank'].isin([1]) & sb_bda_all['BPE_by_channel'].notnull()]
sb_bda_all_2 = sb_bda_all_gr_4.append(sb_bda_all_new, ignore_index = True)
sb_bda_all = sb_bda_all_1.append(sb_bda_all_2, ignore_index = True)
sb_bda_all.to_csv(qc+'sb_bda_all_final.csv')

#%%
sb_bda_all['BPE_by_channel2'] = sb_bda_all.apply(lambda x: x['BPE_by_channel_latest']
            if pd.isna(x['BPE_by_channel1']) else x['BPE_by_channel1'], axis=1)
sb_bda_all['PPE_by_channel2'] = sb_bda_all.apply(lambda x:  x['PPE_by_channel_latest']
            if pd.isna(x['PPE_by_channel1']) else x['PPE_by_channel1'], axis=1)
sb_bda_all['BPE_TotalUS2'] = sb_bda_all.apply(lambda x: x['BPE_TotalUS_latest']
            if pd.isna(x['BPE_TotalUS1']) else x['BPE_TotalUS1'], axis=1)
sb_bda_all['PPE_TotalUS2'] = sb_bda_all.apply(lambda x: x['PPE_TotalUS_latest']
            if pd.isna(x['PPE_TotalUS1']) else x['PPE_TotalUS1'], axis=1)
sb_bda_all.drop(['BPE_by_channel_sum','PPE_by_channel_sum','BPE_TotalUS_sum', 'PPE_TotalUS_sum',
'Base_Statcase_Volume_latest', 'BPE_by_channel_sum', 'PPE_by_channel_sum', 'BPE_TotalUS_sum',
'PPE_TotalUS_sum','BPE_by_channel1','PPE_by_channel1','BPE_TotalUS1','PPE_TotalUS1','unique_rnk'],axis=1,inplace=True)
sb_bda_all.to_csv(output+'trended_bda_FY22Q3_HC.csv')

#%%
sub_brand_bda = sb_bda_latest.copy()
sub_brand_bda.rename(columns={'BPE_by_channel_latest':'BPE_by_channel', 'PPE_by_channel_latest':'PPE_by_channel', 
                              'Base_Statcase_Volume_latest':'Base_Statcase_Volume', 
                              'BPE_TotalUS_latest':'BPE_TotalUS', 'PPE_TotalUS_latest':'PPE_TotalUS'},inplace=True)
sub_brand_bda.to_csv(qc+'sub_brand_bda_hc.csv')

#%%
b_feed_channel_latest = hc_clx.groupby(['Division','Geography','Time Period','rank','SBU','Brand']).apply(sb_elas_latest).reset_index()
b_feed_channel_latest.to_csv(qc+'b_feed_channel_latest_hc.csv')

#%%
# Jishnu - Rank has been removed from groupby for totalUS. This is done BPE_totalUS has same elasticities for a time period.    
b_feed_totalUS_rank_latest = b_feed_channel_latest[['Division','SBU','Brand','Time Period','rank']].drop_duplicates() 
    
#%%
b_feed_BPE_totalUS_latest = b_feed_channel_latest.groupby(['Division','SBU','Brand','Time Period']).apply(lambda x: np.average(x['BPE_by_channel_latest'], weights=x['Base_Statcase_Volume_latest'])).reset_index().rename(columns = {0:'BPE_TotalUS_latest'})
b_feed_PPE_totalUS_latest = b_feed_channel_latest.groupby(['Division','SBU','Brand','Time Period']).apply(lambda x: np.average(x['PPE_by_channel_latest'], weights=x['Base_Statcase_Volume_latest'])).reset_index().rename(columns = {0:'PPE_TotalUS_latest'})

#%%
# Merged with rank level information.    
b_feed_BPE_totalUS_latest = b_feed_totalUS_rank_latest.merge(b_feed_BPE_totalUS_latest, on = ['Division','SBU', 'Brand',
                                                                            'Time Period'], how = 'left')    
b_feed_PPE_totalUS_latest = b_feed_totalUS_rank_latest.merge(b_feed_PPE_totalUS_latest, on = ['Division','SBU', 'Brand',
                                                                            'Time Period'], how = 'left')   
    
#%%
b_bda_BPE_latest = pd.merge(b_feed_channel_latest , b_feed_BPE_totalUS_latest, on = ['Division','SBU',
                                        'Brand','Time Period','rank'], how = 'left' )
brand_bda = pd.merge(b_bda_BPE_latest, b_feed_PPE_totalUS_latest, on = ['Division','SBU', 
                                        'Brand','Time Period','rank'], how = 'left' )
brand_bda.rename(columns={'BPE_by_channel_latest':'BPE_by_channel', 'PPE_by_channel_latest':'PPE_by_channel', 
                          'Base_Statcase_Volume_latest':'Base_Statcase_Volume', 
                          'BPE_TotalUS_latest':'BPE_TotalUS', 'PPE_TotalUS_latest':'PPE_TotalUS'},inplace=True)
brand_bda.to_csv(qc+'brand_bda_hc.csv')

#%%
def pos_agg(x):
    d = {}
    d['Stat Case Baseline Volume'] = x['Stat Case Baseline Volume'].sum()
    d['Stat Case Volume'] = x['Stat Case Volume'].sum()
    d['Dollar Sales'] = x['Dollar Sales'].sum()
    d['Baseline Dollars'] = x['Baseline Dollars'].sum()
    d['Baseline Units'] = x['Baseline Units'].sum()
    d['Baseline Volume'] = x['Baseline Volume'].sum()
    return pd.Series(d, index=['Stat Case Baseline Volume','Stat Case Volume','Dollar Sales','Baseline Dollars',
    'Baseline Units','Baseline Volume'])

#%%
sub_brand_pos = POS_CoefDb_RULE12.groupby(['Division','SBU','Brand','Sub_Brand']).apply(pos_agg).reset_index()
brand_pos = POS_CoefDb_RULE12.groupby(['Division','SBU','Brand']).apply(pos_agg).reset_index()

sub_brand_pos['Retail Price'] = sub_brand_pos['Dollar Sales']/sub_brand_pos['Stat Case Volume']
brand_pos['Retail Price'] = brand_pos['Dollar Sales']/brand_pos['Stat Case Volume']

sub_brand_pos.to_csv(qc+'sub_brand_pos_hc.csv')
brand_pos.to_csv(qc+'brand_pos_hc.csv')

#%%
############# PPL Calculations ####################
# Sub brand Level PPL aggregation
def ppl_agg(x):
    d = {}
    d['Vol'] = x['Vol MSC'].sum()
    d['BCS'] = np.average(x['BCS'], weights=x['Vol MSC'])
    d['Net Real'] = np.average(x['Net Real'], weights=x['Vol MSC'])
    d['CPF'] = np.average(x['CPF'], weights=x['Vol MSC'])
    d['NCS'] = np.average(x['NCS'], weights=x['Vol MSC'])
    d['Contrib'] = np.average(x['Contrib'], weights=x['Vol MSC'])
    d['Gross Profit'] = np.average(x['Gross Profit'], weights=x['Vol MSC'])
    return pd.Series(d, index=['Vol','BCS','Net Real', 'CPF', 'NCS','Contrib', 'Gross Profit'])

#%%
ppl_map = map_xl.parse('PPL')
sub_brand_ppl = ppl_map.groupby(['Division', 'BU', 'Brand Elasticity File','Subbrand Elasticity File']).apply(ppl_agg).reset_index()
brand_ppl = ppl_map.groupby(['Division', 'BU', 'Brand Elasticity File']).apply(ppl_agg).reset_index()

# QC for PPLs
sub_brand_ppl.to_csv(qc+'sub_brand_ppl_hc.csv')
brand_ppl.to_csv(qc+'brand_ppl_hc.csv')

#%%
############# MERGING POS data AND PPL data #################
# Imp step. Check for duplications, blanks

sub_brand_pos_ppl = pd.merge(sub_brand_ppl, sub_brand_pos, left_on = ['Division', 'BU', 'Brand Elasticity File'
,'Subbrand Elasticity File'], right_on = ['Division','SBU','Brand','Sub_Brand'], how = 'left') 
brand_pos_ppl = pd.merge(brand_ppl, brand_pos, left_on = ['Division', 'BU', 'Brand Elasticity File'], 
                              right_on = ['Division','SBU','Brand'], how = 'left') 

# QC for PPLs with #NA POS data
sub_brand_pos_ppl.to_csv(qc+'sub_brand_pos_ppl_hc.csv')
brand_pos_ppl.to_csv(qc+'brand_pos_ppl_hc.csv')

#%%
sub_brand_pos_ppl.drop(['SBU','Brand','Sub_Brand'],axis=1,inplace=True)
brand_pos_ppl.drop(['SBU','Brand'],axis=1,inplace=True)
sub_brand_pos_ppl = sub_brand_pos_ppl[~sub_brand_pos_ppl['Stat Case Baseline Volume'].isnull()]
brand_pos_ppl = brand_pos_ppl[~brand_pos_ppl['Stat Case Baseline Volume'].isnull()]

sub_brand_pos_ppl.to_csv(qc+'sub_brand_pos_ppl_hc_1.csv')
brand_pos_ppl.to_csv(qc+'brand_pos_ppl_hc_1.csv')

#%%
#### Merging BDA, PPL, POS data for Brand and Sub Brands #######
# Imp step. Check for duplications, blanks
sub_brand_final = pd.merge(sub_brand_bda, sub_brand_pos_ppl, left_on=['Division','SBU','Brand','Sub_Brand'],
                           right_on = ['Division','BU','Brand Elasticity File','Subbrand Elasticity File'], how = 'right')
sub_brand_final.to_csv(qc+'sub_brand_final_hc.csv')

brand_final = pd.merge(brand_bda, brand_pos_ppl, left_on=['Division','SBU','Brand'], 
                       right_on = ['Division','BU','Brand Elasticity File'], how = 'right')
brand_final.to_csv(qc+'brand_final_hc.csv')

#%%
sub_brand_final['Product_Name'] = sub_brand_final['Subbrand Elasticity File']
sub_brand_final.drop(['BU','Brand Elasticity File','Subbrand Elasticity File'],axis=1, inplace = True)
brand_final['Product_Name'] = brand_final['Brand Elasticity File']
brand_final.drop(['BU','Brand Elasticity File'],axis=1, inplace = True)

#%%
#appending Brand and sub brand tables
ps_final = sub_brand_final.append(brand_final,ignore_index = True)
ps_final.to_csv(qc+'ps_final_hc.csv')