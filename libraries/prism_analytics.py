#!/usr/bin/env python
# coding: utf-8

# In[333]:


import pandas as pd
import altair as alt
import warnings
warnings.filterwarnings("ignore")
alt.renderers.set_embed_options(theme='light')
pd.set_option("display.max_colwidth", 400)
pd.set_option("display.max_rows", 400)


# Trend line of what yLUNA is being used for?  PRISM Farm, yLUNA Staking, LPing, or Nothing.

# In[334]:


prism_addr = 'terra1dh9478k2qvqhqeajhn75a2a7dsnf74y5ukregw'
yluna_addr = 'terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz'
pluna_addr = 'terra1tlgelulz9pdkhls6uglfn5lmxarx7f2gxtdzh2'
pLuna_PRISM_Pair = 'terra1persuahr6f8fm6nyup0xjc7aveaur89nwgs5vs'
yLuna_PRISM_Pair = 'terra1kqc65n5060rtvcgcktsxycdt2a4r67q2zlvhce'
PRISM_cLUNA_Pair = 'terra1yxgq5y6mw30xy9mmvz9mllneddy9jaxndrphvk'
PRISM_UST_Pair = 'terra19d2alknajcngdezrdhq40h6362k92kz23sz62u'
PRISM_xPRISM_Pair = 'terra1czynvm64nslq2xxavzyrrhau09smvana003nrf'
PRISM_LUNA_Pair = 'terra1r38qlqt69lez4nja5h56qwf4drzjpnu8gz04jd'


# In[335]:


pool_pairs = [pLuna_PRISM_Pair,yLuna_PRISM_Pair,PRISM_cLUNA_Pair,PRISM_LUNA_Pair,PRISM_UST_Pair,PRISM_xPRISM_Pair]


# In[336]:


def claim(claim_hash):
    df = pd.read_json(
            f"https://api.flipsidecrypto.com/api/v2/queries/{claim_hash}/data/latest",
            convert_dates=["BLOCK_TIMESTAMP"])
    df.columns = [c.lower() for c in df.columns]
    return df


# In[337]:


def get_url(url):
    return pd.read_csv(url, index_col=0)


# In[338]:


class ChartProvider:
    def __init__(self):
        pass
    
    def get_yluna_time_area_chart(self, df, scale_, min_date=None, max_date=None, top_padding=0):
        max_date = df['Time'].max()
        df['Amount (millions)'] = round(df['Amount']/1000000,2).apply(str)+'M'
        chart = alt.Chart(df).mark_area().encode(
            x=alt.X('Time:T',scale=alt.Scale(domain=(min_date,max_date))),
            y=alt.X('Amount:Q',scale=alt.Scale(domain=(0,df.groupby(['Time'])['Amount'].sum().max()+top_padding))),
            color=alt.Color('Type:N', 
                        scale=scale_,
                        legend=alt.Legend(
                                    orient='none',
                                    padding=5,
                                    legendY=0,
                                    direction='vertical')),
            tooltip=[alt.Tooltip('Time:T', format='%Y-%m-%d'),'Type:N','Amount (millions):N']
        )
        return chart
    
    def refraction_asset_time(self,all_refreact):
        df = all_refreact.groupby(['day','asset_given']).amount.sum().reset_index()
        df.columns = ['Date', 'Asset refracted', 'Amount']
        df['Amount'] = df.Amount.apply(lambda x: round(x,2))
        df['Amount (k)'] = df.Amount.apply(lambda x: f"{round(x/1000,2)}k")
        df['Asset refracted'] = df['Asset refracted'].map({'LUNA':'LUNA','cLUNA':'cLUNA','yLUNA':'Unrefraction'})
        
        chart = alt.Chart(df).mark_bar().encode(
            x=alt.X('Date:T', sort=alt.EncodingSortField(order='ascending')),
            y="Amount:Q",
            color=alt.Color('Asset refracted', 
                            scale=alt.Scale(scheme='set2'),
                            legend=alt.Legend(
                                    orient='top-right',
                                    padding=5,
                                    legendY=0,
                                    direction='vertical')),
            tooltip=[alt.Tooltip('Date:T', format='%Y-%m-%d %H:%M'), 'Asset refracted', 'Amount (k)']
        ).properties(width=700).configure_axisX(
            labelAngle=0
        ).configure_view(strokeOpacity=0)
        return chart


# In[339]:


class RefractDataProvider:
    def __init__(self, claim, get_url, path_to_data='../data'):
        self.refract = 'c5ac5e60-7da0-429f-98e8-19ccdd77d835'
        self.refract_cluna = '5b0257c3-e93b-49d4-93f6-e370bc3b3f50'
        self.claim = claim
        self.get_url = get_url
        self.path_to_data = path_to_data
    
    def load(self):
        self.load_from_url()
        self.load_from_csv()
        self.refract_df.columns = [c.lower() for c in self.refract_df.columns]
        self.refract_df_from_csv.columns = [c.lower() for c in self.refract_df_from_csv.columns]
        self.refract_cluna_df.columns = [c.lower() for c in self.refract_cluna_df.columns]
        self.refract_cluna_df_from_csv.columns = [c.lower() for c in self.refract_cluna_df_from_csv.columns]
        self.refract_df = self.refract_df.append(self.refract_df_from_csv).drop_duplicates()
        self.refract_cluna_df = self.refract_cluna_df.append(self.refract_cluna_df_from_csv).drop_duplicates()
            
    def load_from_url(self):
        self.refract_df = self.claim(self.refract)
        self.refract_cluna_df = self.claim(self.refract_cluna)
        
    def write_to_csv(self):
        self.refract_df.to_csv(f'{self.path_to_data}/refract.csv')
        self.refract_cluna_df.to_csv(f'{self.path_to_data}/refract_cluna.csv')
        
    def load_from_csv(self):
        self.refract_df_from_csv = pd.read_csv(f'{self.path_to_data}/refract.csv', index_col=0)
        self.refract_cluna_df_from_csv = pd.read_csv(f'{self.path_to_data}/refract_cluna.csv', index_col=0)
        
    def parse_refracting_cluna(self):
        self.refract_cluna_df.columns = [c.lower() for c in self.refract_cluna_df.columns]
        self.refract_cluna_df['user'] = self.refract_cluna_df['from_']
        self.refract_cluna_df_pol = self.refract_cluna_df[['block_timestamp','tx_id','user','0_action','0_amount']]
        self.refract_cluna_df_pol.columns = ['block_timestamp','tx_id','user','action','amount']
        self.refract_cluna_df_pol['asset_given'] = 'cLUNA'
        self.refract_cluna_df_pol['asset_received'] = 'yLUNA'
        self.refract_cluna_df_pol['operation'] = 'refraction'
        self.refract_cluna_df_pol.block_timestamp=self.refract_cluna_df_pol.block_timestamp.apply(str).apply(lambda x: x[:-4] if len(x) == 23 else x)
        self.refract_cluna_df_pol.block_timestamp=self.refract_cluna_df_pol.block_timestamp.apply(str).apply(lambda x: x[:-3] if len(x) == 22 else x)
        self.refract_cluna_df_pol.block_timestamp=self.refract_cluna_df_pol.block_timestamp.apply(str).apply(lambda x: x[:-7] if len(x) == 26 else x) 
        self.refract_cluna_df_pol['hr'] = self.refract_cluna_df_pol.block_timestamp.str[:-5] + '00:00.000'
        self.refract_cluna_df_pol['day'] = self.refract_cluna_df_pol.block_timestamp.str[:-9]
        self.refract_cluna_df_pol['amount_signed'] = self.refract_cluna_df_pol.amount
        self.refract_cluna_df_pol = self.refract_cluna_df_pol.drop_duplicates(['tx_id'],ignore_index=True)
        
        
    def parse_refracting_luna(self):
        self.refract_df.columns = [c.lower() for c in self.refract_df.columns]
        self.refract_df['user'] = self.refract_df.apply(lambda row: row.from_ if row['0_action']=='bond_split' else row.to_, axis=1)
        self.refract_df_pol = self.refract_df[['block_timestamp','tx_id','user','0_action','0_amount']]
        self.refract_df_pol.columns = ['block_timestamp','tx_id','user','action','amount']
        self.refract_df_pol['asset_given'] = self.refract_df_pol.apply(lambda row: 'LUNA' if row['action']=='bond_split' else 'yLUNA',axis=1)
        self.refract_df_pol['asset_received'] = self.refract_df_pol.apply(lambda row: 'yLUNA' if row['action']=='bond_split' else 'LUNA',axis=1)
        self.refract_df_pol['operation'] = 'refraction'
        self.refract_df_pol.block_timestamp=self.refract_df_pol.block_timestamp.apply(str).apply(lambda x: x[:-4] if len(x) == 23 else x)
        self.refract_df_pol.block_timestamp=self.refract_df_pol.block_timestamp.apply(str).apply(lambda x: x[:-3] if len(x) == 22 else x)
        self.refract_df_pol.block_timestamp=self.refract_df_pol.block_timestamp.apply(str).apply(lambda x: x[:-7] if len(x) == 26 else x)
        self.refract_df_pol['hr'] = self.refract_df_pol.block_timestamp.str[:-5] + '00:00.000'
        self.refract_df_pol['day'] = self.refract_df_pol.block_timestamp.str[:-9]
        self.refract_df_pol['amount_signed'] = self.refract_df_pol.apply(lambda row: -row.amount 
                                                        if row.action=='burn_from' else row.amount,axis=1)
        self.refract_df_pol = self.refract_df_pol.drop_duplicates(['tx_id','asset_given','asset_received'],ignore_index=True)
        
    def parse(self):
        self.parse_refracting_cluna()
        self.parse_refracting_luna()
        self.all_refreact = self.refract_df_pol.append(self.refract_cluna_df_pol)
        daily_delta_rf = self.all_refreact.groupby('day').amount_signed.sum().reset_index()
        daily_delta_rf = daily_delta_rf.sort_values(by='day')
        daily_delta_rf['cumsum'] = daily_delta_rf.amount_signed.cumsum().apply(lambda x: round(x,2))
        daily_delta_rf.columns = ['Time', 'Amount signed', 'Amount']
        daily_delta_rf['Type'] = 'yLuna circulating'
        self.daily_delta_rf = daily_delta_rf


# In[340]:


class YLunaStakingDataProvider:
    def __init__(self, claim, get_url, path_to_data='../data'):
        self.ystaking = '3ff0fc49-5a0d-4cdf-a8ab-33f8ea7e755f'
        self.ystaking_farm = '05d91866-0193-4231-b2ca-1774fbd5742a'
        self.claim = claim
        self.get_url = get_url
        self.path_to_data = path_to_data
    
    def load(self):
        self.load_from_url()
        self.load_from_csv()
        self.ystaking_df.columns = [c.lower() for c in self.ystaking_df.columns]
        self.ystaking_df_from_csv.columns = [c.lower() for c in self.ystaking_df_from_csv.columns]
        self.ystaking_farm_df.columns = [c.lower() for c in self.ystaking_farm_df.columns]
        self.ystaking_farm_df_from_csv.columns = [c.lower() for c in self.ystaking_farm_df_from_csv.columns]
        self.ystaking_df = self.ystaking_df.append(self.ystaking_df_from_csv).drop_duplicates()
        self.ystaking_farm_df = self.ystaking_farm_df.append(self.ystaking_farm_df_from_csv)
            
    def load_from_url(self):
        self.ystaking_df = self.claim(self.ystaking)
        self.ystaking_farm_df = self.claim(self.ystaking_farm)
        
    def write_to_csv(self):
        self.ystaking_df.to_csv(f'{self.path_to_data}/ystaking.csv')
        self.ystaking_farm_df.to_csv(f'{self.path_to_data}/ystaking_farm.csv')
        
    def load_from_csv(self):
        self.ystaking_df_from_csv = pd.read_csv(f'{self.path_to_data}/ystaking.csv', index_col=0)
        self.ystaking_farm_df_from_csv = pd.read_csv(f'{self.path_to_data}/ystaking_farm.csv', index_col=0)
        
    def parse_ystaking(self):
        self.ystaking_df.columns = [c.lower() for c in self.ystaking_df.columns]
        self.ystaking_df['action'] = self.ystaking_df.apply(lambda row: row['0_action'] if row['0_action']=='unbond' else row['1_action'],axis=1)
        self.ystaking_df['amount'] = self.ystaking_df.apply(lambda row: row['0_amount'] if row['0_action']=='unbond' else row['1_amount'],axis=1)
        self.ystaking_df['user'] = self.ystaking_df.apply(lambda row: row['to_'] if row.action=='unbond' else row['from_'],axis=1)
        self.ystaking_df['asset_given'] = self.ystaking_df.apply(lambda row: None if row.action=='unbond' else 'yLUNA',axis=1)
        self.ystaking_df['asset_received'] = self.ystaking_df.apply(lambda row: 'yLUNA' if row.action=='unbond' else None,axis=1)
        self.ystaking_df.block_timestamp=self.ystaking_df.block_timestamp.apply(str).apply(lambda x: x[:-4] if len(x) == 23 else x)
        self.ystaking_df.block_timestamp=self.ystaking_df.block_timestamp.apply(str).apply(lambda x: x[:-3] if len(x) == 22 else x)
        self.ystaking_df.block_timestamp=self.ystaking_df.block_timestamp.apply(str).apply(lambda x: x[:-7] if len(x) == 26 else x)
        self.ystaking_df['day'] = self.ystaking_df.hr.apply(str).str[:-13]
        self.ystaking_df = self.ystaking_df[['block_timestamp','tx_id','hr','day','action','amount','user','asset_given','asset_received']]
        self.ystaking_df['amount_signed'] = self.ystaking_df.apply(lambda row: row.amount if row.action=='bond' else -row.amount,axis=1)
        self.ystaking_df = self.ystaking_df.drop_duplicates(['tx_id','asset_given','asset_received'],ignore_index=True)
        
    def parse_ystaking_farm(self):
        self.ystaking_farm_df.columns = [c.lower() for c in self.ystaking_farm_df.columns]
        self.ystaking_farm_df['action'] = self.ystaking_farm_df.apply(lambda row: row['3_action'] if row['3_action']=='bond' else row['1_action'], axis=1)
        self.ystaking_farm_df['amount'] = self.ystaking_farm_df.apply(lambda row: float(row['0_amount']) if row['action']=='bond' else float(row['0_amount']), axis=1)
        self.ystaking_farm_df = self.ystaking_farm_df[['block_timestamp','tx_id','sender','action','amount']]
        self.ystaking_farm_df.block_timestamp=self.ystaking_farm_df.block_timestamp.apply(str).apply(lambda x: x[:-4] if len(x) == 23 else x)
        self.ystaking_farm_df.block_timestamp=self.ystaking_farm_df.block_timestamp.apply(str).apply(lambda x: x[:-3] if len(x) == 22 else x)
        self.ystaking_farm_df.block_timestamp=self.ystaking_farm_df.block_timestamp.apply(str).apply(lambda x: x[:-7] if len(x) == 26 else x)
        self.ystaking_farm_df['hr'] = self.ystaking_farm_df.block_timestamp.str[:-5] + '00:00.000'
        self.ystaking_farm_df['day'] = self.ystaking_farm_df.block_timestamp.str[:-9]
        self.ystaking_farm_df['amount_signed'] = self.ystaking_farm_df.apply(lambda row: row.amount if row.action=='bond' else -row.amount,axis=1)
        self.ystaking_farm_df = self.ystaking_farm_df.drop_duplicates(['tx_id'],ignore_index=True)
    
    def parse(self):
        self.parse_ystaking()
        self.parse_ystaking_farm()


# In[341]:


class SwapsDataProvider:
    def __init__(self, claim, get_url, path_to_data='../data'):
        self.swaps = '1bfd8019-89a1-470d-8868-60d71e57d1d0'
        self.router = '69b149fb-81ba-4860-aac1-b17f0d6d7688'
        self.claim = claim
        self.get_url = get_url
        self.path_to_data = path_to_data
            
    def load(self):
        self.load_from_url()
        self.load_from_csv()
        self.swaps_df.columns = [c.lower() for c in self.swaps_df.columns]
        self.swaps_df_from_csv.columns = [c.lower() for c in self.swaps_df_from_csv.columns]
        self.router_df_from_csv.columns = [c.lower() for c in self.router_df_from_csv.columns]
        self.router_df.columns = [c.lower() for c in self.router_df.columns]
        self.swaps_df = self.swaps_df.append(self.swaps_df_from_csv).drop_duplicates()
        self.router_df = self.router_df.append(self.router_df_from_csv).drop_duplicates()
            
    def load_from_url(self):
        self.swaps_df = self.claim(self.swaps)
        self.router_df = self.claim(self.router)
        
    def write_to_csv(self):
        self.swaps_df.to_csv(f'{self.path_to_data}/swaps.csv')
        self.router_df.to_csv(f'{self.path_to_data}/router.csv')
        
    def load_from_csv(self):
        self.swaps_df_from_csv = pd.read_csv(f'{self.path_to_data}/swaps.csv', index_col=0)
        self.router_df_from_csv = pd.read_csv(f'{self.path_to_data}/router.csv', index_col=0)
        
    def parse_simple_swaps(self):
        self.swaps_df.columns = [c.lower() for c in self.swaps_df.columns]
        swaps_df = self.swaps_df[self.swaps_df.ask_asset != '']
        swaps_df = swaps_df[swaps_df.sender.notna()]
        swaps_df_pol = swaps_df.rename(columns={'sender':'user','ask_asset':'asset_received','offer_asset':'asset_given'})
        swaps_df_pol = swaps_df_pol[['block_timestamp','tx_id','price','user','asset_received','return_amount','asset_given','offer_amount']]
        swaps_df_pol['operation'] = 'swap'
        self.swaps_df_pol = swaps_df_pol.drop_duplicates(['tx_id','asset_given','asset_received'],ignore_index=True)
        
    def parse_router(self):
        self.router_df.columns = [c.lower() for c in self.router_df.columns]
        router_df = self.router_df.rename(columns={'sender':'user'})
        router_df_1 = router_df[['block_timestamp','tx_id','0_ask_asset','0_offer_amount','0_offer_asset','0_price','0_return_amount','user']]
        router_df_1 = router_df_1.rename(columns={'0_ask_asset':'asset_received','0_offer_amount':'offer_amount',
                           '0_price':'price','0_return_amount':'return_amount','0_offer_asset':'asset_given'})
        router_df_2 = router_df[['block_timestamp','tx_id','1_ask_asset','1_offer_amount','1_offer_asset','1_price','1_return_amount','user']]
        router_df_2 = router_df_2.rename(columns={'1_ask_asset':'asset_received','1_offer_amount':'offer_amount',
                           '1_price':'price','1_return_amount':'return_amount','1_offer_asset':'asset_given'})
        router_df_pol = router_df_1.append(router_df_2)
        router_df_pol['operation'] = 'swap'
        self.router_df_pol = router_df_pol.drop_duplicates(['tx_id','asset_given','asset_received'],ignore_index=True)
    
    def parse(self):
        self.parse_simple_swaps()
        self.parse_router()
        self.swaps_df_all = self.router_df_pol.append(self.swaps_df_pol[self.router_df_pol.columns]).drop_duplicates(['tx_id','asset_given','asset_received'],ignore_index=True)
        self.swaps_df_all.block_timestamp=self.swaps_df_all.block_timestamp.apply(str).apply(lambda x: x[:-4] if len(x) == 23 else x)
        self.swaps_df_all.block_timestamp=self.swaps_df_all.block_timestamp.apply(str).apply(lambda x: x[:-3] if len(x) == 22 else x)
        self.swaps_df_all.block_timestamp=self.swaps_df_all.block_timestamp.apply(str).apply(lambda x: x[:-7] if len(x) == 26 else x)
        self.swaps_df_all['hr'] = self.swaps_df_all.block_timestamp.str[:-5] + '00:00.000'
        self.swaps_df_all['day'] = self.swaps_df_all.block_timestamp.str[:-9]
        #
        yluna_swaps = self.swaps_df_all[(self.swaps_df_all.asset_given=='yLUNA')|(self.swaps_df_all.asset_received=='yLUNA')]
        yluna_swaps['amount_signed'] = yluna_swaps.apply(lambda row: row.offer_amount if row.asset_given == 'yLUNA' else -row.return_amount,axis=1)
        yluna_swaps['type'] = 'swap'
        self.yluna_swaps = yluna_swaps.drop_duplicates(ignore_index=True)
        #
        pluna_swaps = self.swaps_df_all[(self.swaps_df_all.asset_given=='pLUNA')|(self.swaps_df_all.asset_received=='pLUNA')]
        pluna_swaps['amount_signed'] = pluna_swaps.apply(lambda row: row.offer_amount if row.asset_given == 'pLUNA' else -row.return_amount,axis=1)
        pluna_swaps['type'] = 'swap'
        self.pluna_swaps = pluna_swaps.drop_duplicates(ignore_index=True)
        


# In[342]:


class LPDataProvider:
    def __init__(self, claim, get_url, path_to_data='../data'):
        self.lp_provide_withdraw = '7f5c7008-648a-4944-9fe9-e6c37e2e7bb8'
        self.claim = claim
        self.get_url = get_url
        self.path_to_data = path_to_data
            
    def load(self):
        self.load_from_url()
        self.load_from_csv()
        self.lp_provide_withdraw_df.columns = [c.lower() for c in self.lp_provide_withdraw_df.columns]
        self.lp_provide_withdraw_df_from_csv.columns = [c.lower() for c in self.lp_provide_withdraw_df_from_csv.columns]
        self.lp_provide_withdraw_df = self.lp_provide_withdraw_df.append(self.lp_provide_withdraw_df_from_csv).drop_duplicates()
            
    def load_from_url(self):
        self.lp_provide_withdraw_df = self.claim(self.lp_provide_withdraw)
        
    def write_to_csv(self):
        self.lp_provide_withdraw_df.to_csv(f'{self.path_to_data}/lp_provide_withdraw.csv')
        
    def load_from_csv(self):
        self.lp_provide_withdraw_df_from_csv = pd.read_csv(f'{self.path_to_data}/lp_provide_withdraw.csv', index_col=0)
        
    def get_action(self, row):
        for i in range(-1,6):
            prefix = f"{i}_" if i >= 0 else ""
            if(f'{prefix}action' in row):
                if(row[f'{prefix}action'] in ['provide_liquidity','withdraw_liquidity']):
                    return row[f'{prefix}action']
                
    def get_n_action(self, row):
        for i in range(-1,6):
            prefix = f"{i}_" if i >= 0 else ""
            if(f'{prefix}action' in row):
                if(row[f'{prefix}action'] in ['provide_liquidity','withdraw_liquidity']):
                    return f'{prefix}'
                
    def correct_parsing(self, row):
        if (row.prefix == '1_' and row.f_action == 'withdraw_liquidity') or             (row.prefix == '0_' and row.f_action == 'provide_liquidity'):
            return False
        else:
            return True
                
    def parse(self):
        self.lp_provide_withdraw_df.columns = [c.lower() for c in self.lp_provide_withdraw_df.columns]
        df = self.lp_provide_withdraw_df
        df['f_action'] = df.apply(self.get_action,axis=1)
        df['prefix'] = df.apply(self.get_n_action,axis=1)
        df.block_timestamp=df.block_timestamp.apply(str).apply(lambda x: x[:-4] if len(x) == 23 else x)
        df.block_timestamp=df.block_timestamp.apply(str).apply(lambda x: x[:-3] if len(x) == 22 else x)
        df.block_timestamp=df.block_timestamp.apply(str).apply(lambda x: x[:-7] if len(x) == 26 else x) 
        df['f_contract_address'] = df.apply(lambda row: row[f'{row.prefix}contract_address'], axis=1)
        df['asset'] = df.apply(lambda row: 'yLuna' if row.f_contract_address=='terra1kqc65n5060rtvcgcktsxycdt2a4r67q2zlvhce'                                     else 'pLuna', axis=1)
        assert df.apply(self.correct_parsing , axis=1).sum() == 0
        provide_ = df[df.f_action=='provide_liquidity']
        withdraw_ = df[df.f_action=='withdraw_liquidity']
        #
        provide_['prism_amount'] = provide_.apply(lambda row: row['2_amount'] if row['1_contract_address'] == prism_addr else row['3_amount'],axis=1)
        provide_['asset_amount'] = provide_.apply(lambda row: row['1_amount'] if row['2_contract_address'] in [yluna_addr,pluna_addr] else row['2_amount'],axis=1)
        provide_['sender'] = provide_['from_']
        provide_['hr'] = provide_.block_timestamp.str[:-5] + '00:00.000'
        provide_['day'] = provide_.block_timestamp.str[:-9]
        provide_ = provide_[['block_timestamp','sender','tx_id','f_action','prism_amount','asset','asset_amount','hr','day']]
        provide_['amount_signed'] = provide_.asset_amount
        provide_['type'] = 'provide_lp'
        self.provide_ = provide_.drop_duplicates(ignore_index=True).drop_duplicates(['tx_id','asset'],ignore_index=True)
        #
        withdraw_['prism_amount'] = withdraw_.apply(lambda row: row['1_amount'] if row['2_contract_address'] == prism_addr else row['2_amount'],axis=1)
        withdraw_['asset_amount'] = withdraw_.apply(lambda row: row['2_amount'] if row['3_contract_address'] in [yluna_addr,pluna_addr] else row['1_amount'],axis=1)
        withdraw_['sender'] = withdraw_['from_']
        withdraw_['hr'] = withdraw_.block_timestamp.str[:-5] + '00:00.000'
        withdraw_['day'] = withdraw_.block_timestamp.str[:-9]
        withdraw_ = withdraw_[['block_timestamp','sender','tx_id','f_action','prism_amount','asset','asset_amount','hr','day']]
        withdraw_['amount_signed'] = -withdraw_.asset_amount
        withdraw_['type'] = 'withdraw_lp'
        self.withdraw_ = withdraw_.drop_duplicates(ignore_index=True).drop_duplicates(['tx_id','asset'],ignore_index=True)


# In[343]:


class CollectorDataProvider:
    def __init__(self, claim, get_url, path_to_data='../data'):
        self.collector = '2ab62a07-3882-48e6-bdc6-d9e592aee2d8'
        self.claim = claim
        self.get_url = get_url
        self.path_to_data = path_to_data
            
    def load(self):
        self.load_from_url()
        self.load_from_csv()
        self.collector_df.columns = [c.lower() for c in self.collector_df.columns]
        self.collector_df_from_csv.columns = [c.lower() for c in self.collector_df_from_csv.columns]
        self.collector_df = self.collector_df.append(self.collector_df_from_csv).drop_duplicates()
               
    def load_from_url(self):
        self.collector_df = self.claim(self.collector)
        
    def write_to_csv(self):
        self.collector_df.to_csv(f'{self.path_to_data}/collector.csv')
        
    def load_from_csv(self):
        self.collector_df_from_csv = pd.read_csv(f'{self.path_to_data}/collector.csv', index_col=0)
        
    def get_amount_asset(self,row, pair_addr, asset_addr):
        #Swapping Prism for yLuna
        for i in range(-1,6):
            prefix = f"{i}_" if i >= 0 else ""
            addr = str(row[f'{prefix}offer_asset']).replace('cw20:','') if row[f'{prefix}offer_asset'] else ''
            if(addr in [asset_addr]):
                return row[f'{prefix}offer_amount']
        #Asking for yLuna from the pool
        for i in range(-1,6):
            prefix = f"{i}_" if i >= 0 else ""
            addr = str(row[f'{prefix}ask_asset']).replace('cw20:','') if row[f'{prefix}ask_asset'] else ''
            if(addr in [asset_addr]):
                return -row[f'{prefix}return_amount']
        #Swapping PRISM for yLuna
        for i in range(-1,6):
            prefix = f"{i}_" if i >= 0 else ""
            if(row[f'{prefix}contract_address'] in [pair_addr]):
                if(row[f'{prefix}from'] == [pair_addr]):
                    if(row[f'{prefix}action'] == 'swap'):
                        return row[f'{i}_amount']
        #Sending yLuna to the pool
        for i in range(-1,6):
            prefix = f"{i}_" if i >= 0 else ""
            if(row[f'{prefix}contract_address'] == prism_addr):
                if(row[f'{prefix}from'] in [pair_addr]):
                    if(row[f'{prefix}action'] == 'send'):
                        return row[f'{prefix}amount']
        return None

    def get_contract(self,row, pair_addr, asset_addr):
        #Swapping Prism for yLuna
        for i in range(-1,6):
            prefix = f"{i}_" if i >= 0 else ""
            addr = str(row[f'{prefix}offer_asset']).replace('cw20:','') if row[f'{prefix}offer_asset'] else ''
            if(addr in [asset_addr]):
                return addr
        #Asking for yLuna from the pool
        for i in range(-1,6):
            prefix = f"{i}_" if i >= 0 else ""
            addr = str(row[f'{prefix}ask_asset']).replace('cw20:','') if row[f'{prefix}ask_asset'] else ''
            if(addr in [asset_addr]):
                return addr
        #Swapping PRISM for yLuna
        for i in range(-1,6):
            prefix = f"{i}_" if i >= 0 else ""
            if(row[f'{prefix}contract_address'] in [pair_addr]):
                if(row[f'{prefix}from'] == [pair_addr]):
                    if(row[f'{prefix}action'] == 'swap'):
                        return row[f'{prefix}contract_address']
        #Sending yLuna to the pool
        for i in range(-1,6):
            prefix = f"{i}_" if i >= 0 else ""
            if(row[f'{prefix}contract_address'] == prism_addr):
                if(row[f'{prefix}from'] in [pair_addr]):
                    if(row[f'{prefix}action'] == 'send'):
                        return row[f'{prefix}from']
        return None
    
    def parse_asset(self, df, asset_addr, pair_addr, asset_name):
        df = df.copy()
        df['amount_signed'] = df.apply(self.get_amount_asset,args=(pair_addr, asset_addr),axis=1)/1000000
        df['f_contract'] = df.apply(self.get_contract,args=(pair_addr, asset_addr),axis=1)
        df = df[df.f_contract.notna()]
        df['asset'] = asset_name
        return df
        
        
    def parse(self,withdraw_,provide_,swaps):
        self.lp_txs = withdraw_[['tx_id']].append(provide_[['tx_id']])                            .append(swaps[['tx_id']])
        self.lp_txs.columns = [c.lower() for c in self.lp_txs.columns]
        self.collector_df.columns = [c.lower() for c in self.collector_df.columns]
        collector_df = self.collector_df
        collector_df.block_timestamp=collector_df.block_timestamp.apply(str).apply(lambda x: x[:-4] if len(x) == 23 else x)
        collector_df.block_timestamp=collector_df.block_timestamp.apply(str).apply(lambda x: x[:-3] if len(x) == 22 else x)
        collector_df.block_timestamp=collector_df.block_timestamp.apply(str).apply(lambda x: x[:-7] if len(x) == 26 else x) 
        collector_df['hr'] = collector_df.block_timestamp.str[:-5] + '00:00.000'
        collector_df['day'] = collector_df.block_timestamp.str[:-9]
        collector_df = collector_df[collector_df.tx_id.isin(set(self.collector_df.tx_id.unique()).difference(self.lp_txs.tx_id.unique()))]
        collector_df['type'] = 'collector_and_other'
        self.collector_df = collector_df
        df_pluna = self.parse_asset(self.collector_df, pluna_addr, pLuna_PRISM_Pair, 'pLuna').drop_duplicates(['tx_id','asset'],ignore_index=True)
        df_yluna = self.parse_asset(self.collector_df, yluna_addr, yLuna_PRISM_Pair, 'yLuna').drop_duplicates(['tx_id','asset'],ignore_index=True)
        self.collector_pyluna = df_pluna.append(df_yluna).drop_duplicates(ignore_index=True)
        
        assert len(self.collector_pyluna) > 0
        assert len(self.collector_pyluna[~(self.collector_pyluna.f_contract.isin(                    [yLuna_PRISM_Pair,pLuna_PRISM_Pair,yluna_addr,pluna_addr]))]) == 0
        assert self.collector_pyluna[(self.collector_pyluna.day < '2022-03-10') &
                                     ~(self.collector_pyluna.tx_id.isin(['F1371E3F465062633DF33F66CAA3CD8BF430FB010C38EFDE7F99ECBB4D3F6FD3']))]\
                ['amount_signed'].isna().sum() == 0
        


# In[344]:


class DataProvider:
    def __init__(self, asset_name, path_to_data='../data'):
        self.path_to_data = path_to_data
        self.asset_name = asset_name
        self.dates_to_mark = pd.DataFrame([
            ['2022-02-13', '2022-02-13',2000000,'Prism Forge'], 
            ['2022-03-06', '2022-03-06',2400000,'Prism Farm']], 
            columns=['text_date','date','height','text']
        )
    def lp_delta(self, withdraw_, provide_, yluna_swaps, collector_df):
        self.all_lps = withdraw_[['day','amount_signed','tx_id','type','block_timestamp']]                            .append(provide_[['day','amount_signed','tx_id','type','block_timestamp']])                            .append(yluna_swaps[['day','amount_signed','tx_id','type','block_timestamp']])                            .append(collector_df[['day','amount_signed','tx_id','type','block_timestamp']]).drop_duplicates(ignore_index=True)
        daily_delta_lp = self.all_lps.groupby('day').amount_signed.sum().reset_index()
        daily_delta_lp = daily_delta_lp.sort_values(by='day')
        daily_delta_lp['cumsum'] = daily_delta_lp.amount_signed.cumsum().apply(lambda x: round(x,2))
        daily_delta_lp.columns = ['Time', 'Amount signed', 'Amount']
        daily_delta_lp['Type'] = f'{self.asset_name} LP'
        self.daily_delta_lp = daily_delta_lp
        
    def stk_farm_delta(self, ystaking_farm_df):
        daily_delta_stk_farm = ystaking_farm_df.groupby('day').amount_signed.sum().reset_index()
        daily_delta_stk_farm = daily_delta_stk_farm.sort_values(by='day')
        daily_delta_stk_farm['cumsum'] = daily_delta_stk_farm.amount_signed.cumsum().apply(lambda x: round(x,2))
        daily_delta_stk_farm.columns = ['Time', 'Amount signed', 'Amount']
        daily_delta_stk_farm['Type'] = f'{self.asset_name} Farm staked'
        self.daily_delta_stk_farm = daily_delta_stk_farm
        
    def stk_delta(self, ystaking_df):
        daily_delta_stk = ystaking_df.groupby('day').amount_signed.sum().reset_index()
        daily_delta_stk = daily_delta_stk.sort_values(by='day')
        daily_delta_stk['cumsum'] = daily_delta_stk.amount_signed.cumsum().apply(lambda x: round(x,2))
        daily_delta_stk.columns = ['Time', 'Amount signed', 'Amount']
        daily_delta_stk['Type'] = f'{self.asset_name} staked'
        self.daily_delta_stk = daily_delta_stk
        
    def refact_delta(self, all_refreact):
        daily_delta_rf = all_refreact.groupby('day').amount_signed.sum().reset_index()
        daily_delta_rf = daily_delta_rf.sort_values(by='day')
        daily_delta_rf['cumsum'] = daily_delta_rf.amount_signed.cumsum().apply(lambda x: round(x,2))
        daily_delta_rf.columns = ['Time', 'Amount signed', 'Amount']
        daily_delta_rf['Type'] = f'{self.asset_name} circulating'
        self.daily_delta_rf = daily_delta_rf
    
    def unused_asset(self, all_deltas):
        self.asset_used = all_deltas[self.all_deltas.Type.isin([f'{self.asset_name} LP',f'{self.asset_name} staked',f'{self.asset_name} Farm staked'])]
        asset_tot = self.all_deltas[self.all_deltas.Type.isin([f'{self.asset_name} circulating'])]
        asset_unused = self.asset_used.groupby('Time').Amount.sum().rename('Amount_used').reset_index().merge(asset_tot, on='Time')
        asset_unused['Amount_delta'] = asset_unused['Amount'] - asset_unused['Amount_used']
        asset_unused['Amount_delta'] = asset_unused['Amount_delta'].apply(lambda x: x if x>0 else 0)
        asset_unused = asset_unused[['Time','Amount signed','Amount_delta','Type']]
        asset_unused['Type'] = f'{self.asset_name} unused'
        asset_unused.columns = ['Time','Amount signed','Amount','Type']
        self.asset_unused = asset_unused
        
    def all_delta(self):
        self.all_deltas = self.daily_delta_rf.append(self.daily_delta_lp)
        try:
            self.all_deltas = self.all_deltas.append(self.daily_delta_stk)
        except:
            pass
        try:
            self.all_deltas = self.all_deltas.append(self.daily_delta_stk_farm)
        except:
            pass
        
    def fill_date_gaps(self, dff, extra_dates=[]):
        dd = dff.Time.unique()
        dd = [*dd,*extra_dates]
        unique_dates = pd.Series(dd).rename('Time').reset_index().drop(columns='index')
        for t in dff.Type.unique():
            df = dff[dff.Type==t]
            last_date = df.Time.max()
            last_value = df[df.Time==last_date].Amount.values[0]
            df = df.merge(unique_dates, on='Time', how='right')
            df = df[df.Type.isna()]
            if(len(df)>0):
                df['Type'] = t
                df['Amount'] = df.apply(lambda row: last_value if row.Time>last_date else 0,axis=1)
            dff = dff.append(df.fillna(0))
        return dff
