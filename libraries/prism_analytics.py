#!/usr/bin/env python
# coding: utf-8

# In[305]:


import pandas as pd
import altair as alt
import warnings
warnings.filterwarnings("ignore")
alt.renderers.set_embed_options(theme='light')
pd.set_option("display.max_colwidth", 400)


# Trend line of what yLUNA is being used for?  PRISM Farm, yLUNA Staking, LPing, or Nothing.

# In[431]:


class DataProvider:
    def __init__(self, claim, get_url, path_to_data='../data'):
        self.refract = 'c5ac5e60-7da0-429f-98e8-19ccdd77d835'
        self.refract_cluna = '5b0257c3-e93b-49d4-93f6-e370bc3b3f50'
        self.lping = '879d0260-93a9-4576-a1bd-2cc3dc26bf13'
        self.ystaking = '3ff0fc49-5a0d-4cdf-a8ab-33f8ea7e755f'
        self.ystaking_farm = '05d91866-0193-4231-b2ca-1774fbd5742a'
        self.swaps = 'a3b3ad75-15b2-4a40-9850-00e8c1c33ef0'
        self.router = '69b149fb-81ba-4860-aac1-b17f0d6d7688'
        self.claim = claim
        self.get_url = get_url
        self.path_to_data = path_to_data
        
    def load_from_url(self):
        self.ystaking_farm_df = self.claim(self.ystaking_farm)
        self.lping_df = self.claim(self.lping)
        self.refract_df = self.claim(self.refract)
        self.refract_cluna_df = self.claim(self.refract_cluna)
        self.ystaking_df = self.claim(self.ystaking)
        self.swaps_df = self.claim(self.swaps)
        self.router_df = self.claim(self.router)
    
    def write_to_csv(self):
        self.ystaking_farm_df.to_csv(f'{self.path_to_data}/ystaking_farm.csv')
        self.lping_df.to_csv(f'{self.path_to_data}/lping.csv')
        self.refract_df.to_csv(f'{self.path_to_data}a/refract.csv')
        self.refract_cluna_df.to_csv(f'{self.path_to_data}/refract_cluna.csv')
        self.ystaking_df.to_csv(f'{self.path_to_data}/ystaking.csv')
        self.swaps_df.to_csv(f'{self.path_to_data}/swaps.csv')
        self.router_df.to_csv(f'{self.path_to_data}/router.csv')
        
    def load_from_csv(self):
        self.ystaking_farm_df = pd.read_csv(f'{self.path_to_data}/ystaking_farm.csv', index_col=0)
        self.lping_df = pd.read_csv(f'{self.path_to_data}/lping.csv', index_col=0)
        self.refract_df = pd.read_csv(f'{self.path_to_data}/refract.csv', index_col=0)
        self.refract_cluna_df = pd.read_csv(f'{self.path_to_data}/refract_cluna.csv', index_col=0)
        self.ystaking_df = pd.read_csv(f'{self.path_to_data}/ystaking.csv', index_col=0)
        self.swaps_df = pd.read_csv(f'{self.path_to_data}/swaps.csv', index_col=0)
        self.router_df = pd.read_csv(f'{self.path_to_data}/router.csv', index_col=0)
        
    def polish_lping(self):
        self.lping_df['action'] = self.lping_df.apply(lambda row: row['0_action'] if row['0_action'] == 'provide_liquidity'                                             else row['1_action'],axis=1)
        provide_ = self.lping_df[self.lping_df.action=='provide_liquidity']
        withdraw_ = self.lping_df[self.lping_df.action=='withdraw_liquidity']
        #
        provide_['prism_amount'] = provide_.apply(lambda row: row['2_amount'] if row['1_contract_address'] == 'terra1dh9478k2qvqhqeajhn75a2a7dsnf74y5ukregw' else row['3_amount'],axis=1)
        provide_['yluna_amount'] = provide_.apply(lambda row: row['1_amount'] if row['2_contract_address'] == 'terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz' else row['2_amount'],axis=1)
        provide_['sender'] = provide_['from_']
        provide_['hr'] = provide_.block_timestamp.apply(str).str[:-9] + '00:00.000'
        provide_['day'] = provide_.block_timestamp.apply(str).str[:-13]
        provide_ = provide_[['block_timestamp','sender','tx_id','action','prism_amount','yluna_amount','hr','day']]
        provide_['amount_signed'] = provide_.yluna_amount
        #
        withdraw_['prism_amount'] = withdraw_.apply(lambda row: row['1_amount'] if row['2_contract_address'] == 'terra1dh9478k2qvqhqeajhn75a2a7dsnf74y5ukregw' else row['2_amount'],axis=1)
        withdraw_['yluna_amount'] = withdraw_.apply(lambda row: row['2_amount'] if row['3_contract_address'] == 'terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz' else row['1_amount'],axis=1)
        withdraw_['sender'] = withdraw_['from_']
        withdraw_['hr'] = withdraw_.block_timestamp.apply(str).str[:-9] + '00:00.000'
        withdraw_['day'] = withdraw_.block_timestamp.apply(str).str[:-13]
        withdraw_ = withdraw_[['block_timestamp','sender','tx_id','action','prism_amount','yluna_amount','hr','day']]
        withdraw_['amount_signed'] = -withdraw_.yluna_amount
        #
        self.all_lps = withdraw_.append(provide_)
        daily_delta_lp = self.all_lps.groupby('day').amount_signed.sum().reset_index()
        daily_delta_lp = daily_delta_lp.sort_values(by='day')
        daily_delta_lp['cumsum'] = daily_delta_lp.amount_signed.cumsum().apply(lambda x: round(x,2))
        daily_delta_lp.columns = ['Time', 'Amount signed', 'Amount']
        daily_delta_lp['Type'] = 'yLuna LP'
        self.daily_delta_lp = daily_delta_lp
        
    def polish_refracting_cluna(self):
        self.refract_cluna_df['user'] = self.refract_cluna_df['from_']
        self.refract_cluna_df_pol = self.refract_cluna_df[['block_timestamp','tx_id','user','0_action','0_amount']]
        self.refract_cluna_df_pol.columns = ['block_timestamp','tx_id','user','action','amount']
        self.refract_cluna_df_pol['asset_given'] = 'cLUNA'
        self.refract_cluna_df_pol['asset_received'] = 'yLUNA'
        self.refract_cluna_df_pol['operation'] = 'refraction'
        self.refract_cluna_df_pol.block_timestamp = self.refract_cluna_df_pol.block_timestamp.apply(str)                                                    .apply(lambda t: t if len(t) < 26 else t[:-7])
        self.refract_cluna_df_pol['hr'] = self.refract_cluna_df_pol.block_timestamp.str[:-9] + '00:00.000'
        self.refract_cluna_df_pol['day'] = self.refract_cluna_df_pol.block_timestamp.str[:-13]
        self.refract_cluna_df_pol['amount_signed'] = self.refract_cluna_df_pol.amount
        
    def polish_refracting_luna(self):
        self.refract_df['user'] = self.refract_df.apply(lambda row: row.from_ if row['0_action']=='bond_split' else row.to_, axis=1)
        self.refract_df_pol = self.refract_df[['block_timestamp','tx_id','user','0_action','0_amount']]
        self.refract_df_pol.columns = ['block_timestamp','tx_id','user','action','amount']
        self.refract_df_pol['asset_given'] = self.refract_df_pol.apply(lambda row: 'LUNA' if row['action']=='bond_split' else 'yLUNA',axis=1)
        self.refract_df_pol['asset_received'] = self.refract_df_pol.apply(lambda row: 'yLUNA' if row['action']=='bond_split' else 'LUNA',axis=1)
        self.refract_df_pol['operation'] = 'refraction'
        self.refract_df_pol.block_timestamp = self.refract_df_pol.block_timestamp.apply(str)                                                    .apply(lambda t: t if len(t) < 26 else t[:-7])
        self.refract_df_pol['hr'] = self.refract_df_pol.block_timestamp.str[:-9] + '00:00.000'
        self.refract_df_pol['day'] = self.refract_df_pol.block_timestamp.str[:-13]
        self.refract_df_pol['amount_signed'] = self.refract_df_pol.apply(lambda row: -row.amount 
                                                        if row.action=='burn_from' else row.amount,axis=1)
        
    def polish_refracting(self):
        self.polish_refracting_cluna()
        self.polish_refracting_luna()
        self.all_refreact = self.refract_df_pol.append(self.refract_cluna_df_pol)
        daily_delta_rf = self.all_refreact.groupby('day').amount_signed.sum().reset_index()
        daily_delta_rf = daily_delta_rf.sort_values(by='day')
        daily_delta_rf['cumsum'] = daily_delta_rf.amount_signed.cumsum().apply(lambda x: round(x,2))
        daily_delta_rf.columns = ['Time', 'Amount signed', 'Amount']
        daily_delta_rf['Type'] = 'yLuna circulating'
        self.daily_delta_rf = daily_delta_rf
        
    def polish_ystaking(self):
        self.ystaking_df['action'] = self.ystaking_df.apply(lambda row: row['0_action'] if row['0_action']=='unbond' else row['1_action'],axis=1)
        self.ystaking_df['amount'] = self.ystaking_df.apply(lambda row: row['0_amount'] if row['0_action']=='unbond' else row['1_amount'],axis=1)
        self.ystaking_df['user'] = self.ystaking_df.apply(lambda row: row['to_'] if row.action=='unbond' else row['from_'],axis=1)
        self.ystaking_df['asset_given'] = self.ystaking_df.apply(lambda row: None if row.action=='unbond' else 'yLUNA',axis=1)
        self.ystaking_df['asset_received'] = self.ystaking_df.apply(lambda row: 'yLUNA' if row.action=='unbond' else None,axis=1)
        self.ystaking_df['day'] = self.ystaking_df.hr.apply(str).str[:-13]
        self.ystaking_df = self.ystaking_df[['block_timestamp','tx_id','hr','day','action','amount','user','asset_given','asset_received']]
        self.ystaking_df['amount_signed'] = self.ystaking_df.apply(lambda row: row.amount if row.action=='bond' else -row.amount,axis=1)

        daily_delta_stk = self.ystaking_df.groupby('day').amount_signed.sum().reset_index()
        daily_delta_stk = daily_delta_stk.sort_values(by='day')
        daily_delta_stk['cumsum'] = daily_delta_stk.amount_signed.cumsum().apply(lambda x: round(x,2))
        daily_delta_stk.columns = ['Time', 'Amount signed', 'Amount']
        daily_delta_stk['Type'] = 'yLuna staked'
        self.daily_delta_stk = daily_delta_stk
        
    def polish_ystaking_farm(self):
        self.ystaking_farm_df['action'] = self.ystaking_farm_df.apply(lambda row: row['3_action'] if row['3_action']=='bond'                                                                                          else row['1_action'], axis=1)
        self.ystaking_farm_df['amount'] = self.ystaking_farm_df.apply(lambda row: float(row['0_amount']) if row['action']=='bond'                                                                                                  else float(row['0_amount']), axis=1)
        self.ystaking_farm_df = self.ystaking_farm_df[['block_timestamp','tx_id','sender','action','amount']]
        self.ystaking_farm_df['hr'] = self.ystaking_farm_df.block_timestamp.apply(str).str[:-9] + '00:00.000'
        self.ystaking_farm_df['day'] = self.ystaking_farm_df.block_timestamp.apply(str).str[:-13]
        self.ystaking_farm_df['amount_signed'] = self.ystaking_farm_df.apply(lambda row: row.amount if row.action=='bond' else -row.amount,axis=1)
        
        daily_delta_stk_farm = self.ystaking_farm_df.groupby('day').amount_signed.sum().reset_index()
        daily_delta_stk_farm = daily_delta_stk_farm.sort_values(by='day')
        daily_delta_stk_farm['cumsum'] = daily_delta_stk_farm.amount_signed.cumsum().apply(lambda x: round(x,2))
        daily_delta_stk_farm.columns = ['Time', 'Amount signed', 'Amount']
        daily_delta_stk_farm['Type'] = 'yLuna Farm staked'
        self.daily_delta_stk_farm = daily_delta_stk_farm
        
        
    def polish(self):
        self.polish_lping()
        self.polish_refracting()
        self.polish_ystaking()
        self.polish_ystaking_farm()
        
  
class ChartProvider:
    def __init__(self):
        pass
    
    def get_line_chart(self, df, domain=[], range_=[], min_date=None, max_date=None, top_padding=0):
        max_date = df['Time'].max()
        chart = alt.Chart(df).mark_line(point = True).encode(
            x=alt.X('Time:T',scale=alt.Scale(domain=(min_date,max_date))),
            y=alt.X('Amount:Q',scale=alt.Scale(domain=(0,df['Amount'].max()+top_padding))),
            color=alt.Color('Type:N', 
                        sort=domain,
                        scale=alt.Scale(domain=domain, range=range_),
                        legend=alt.Legend(
                                    orient='none',
                                    padding=5,
                                    legendY=0,
                                    direction='horizontal')),
            tooltip=[alt.Tooltip('Time:T', format='%Y-%m-%d %H:%M'),'Amount:Q']
        ).properties(width=800).configure_view(strokeOpacity=0)
        return chart