#!/usr/bin/env python
# coding: utf-8

# In[152]:


import pandas as pd
import altair as alt
import warnings
warnings.filterwarnings("ignore")
alt.renderers.set_embed_options(theme='light')


# Trend line of what yLUNA is being used for?  PRISM Farm, yLUNA Staking, LPing, or Nothing.

# In[153]:


def claim(claim_hash):
    df = pd.read_json(
            f"https://api.flipsidecrypto.com/api/v2/queries/{claim_hash}/data/latest",
            convert_dates=["BLOCK_TIMESTAMP"])
    df.columns = [c.lower() for c in df.columns]
    return df


# In[154]:


def get_url(url):
    return pd.read_csv(url, index_col=0)


# In[155]:


refract = 'c5ac5e60-7da0-429f-98e8-19ccdd77d835'
refract_cluna = '5b0257c3-e93b-49d4-93f6-e370bc3b3f50'
lping = '879d0260-93a9-4576-a1bd-2cc3dc26bf13'
ystaking = '3ff0fc49-5a0d-4cdf-a8ab-33f8ea7e755f'
swaps = 'a3b3ad75-15b2-4a40-9850-00e8c1c33ef0'
router = '69b149fb-81ba-4860-aac1-b17f0d6d7688'


# lping_df = claim(lping)
# refract_df = claim(refract)
# refract_cluna_df = claim(refract_cluna)
# ystaking_df = claim(ystaking)
# swaps_df = claim(swaps)
# router_df = claim(router)

# lping_df.to_csv('../data/lping.csv')
# refract_df.to_csv('../data/refract.csv')
# refract_cluna_df.to_csv('../data/refract_cluna.csv')
# ystaking_df.to_csv('../data/ystaking.csv')
# swaps_df.to_csv('../data/swaps.csv')
# router_df.to_csv('../data/router.csv')

# In[156]:


lping_df = pd.read_csv('../data/lping.csv', index_col=0)
refract_df = pd.read_csv('../data/refract.csv', index_col=0)
refract_cluna_df = pd.read_csv('../data/refract_cluna.csv', index_col=0)
ystaking_df = pd.read_csv('../data/ystaking.csv', index_col=0)
swaps_df = pd.read_csv('../data/swaps.csv', index_col=0)
router_df = pd.read_csv('../data/router.csv', index_col=0)


# ### LPing

# In[157]:


lping_df['action'] = lping_df.apply(lambda row: row['0_action'] if row['0_action'] == 'provide_liquidity' else row['1_action'],axis=1)


# In[158]:


provide_ = lping_df[lping_df.action=='provide_liquidity']
withdraw_ = lping_df[lping_df.action=='withdraw_liquidity']


# In[159]:


provide_['prism_amount'] = provide_.apply(lambda row: row['2_amount'] if row['1_contract_address'] == 'terra1dh9478k2qvqhqeajhn75a2a7dsnf74y5ukregw' else row['3_amount'],axis=1)
provide_['yluna_amount'] = provide_.apply(lambda row: row['1_amount'] if row['2_contract_address'] == 'terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz' else row['2_amount'],axis=1)
provide_['sender'] = provide_['from_']
provide_['hr'] = provide_.block_timestamp.str[:-9] + '00:00.000'
provide_['day'] = provide_.block_timestamp.str[:-13]
provide_ = provide_[['block_timestamp','sender','tx_id','action','prism_amount','yluna_amount','hr','day']]
provide_['amount_signed'] = provide_.yluna_amount
provide_


# In[160]:


pd.set_option("display.max_colwidth", 400)
withdraw_['prism_amount'] = withdraw_.apply(lambda row: row['1_amount'] if row['2_contract_address'] == 'terra1dh9478k2qvqhqeajhn75a2a7dsnf74y5ukregw' else row['2_amount'],axis=1)
withdraw_['yluna_amount'] = withdraw_.apply(lambda row: row['2_amount'] if row['3_contract_address'] == 'terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz' else row['1_amount'],axis=1)
withdraw_['sender'] = withdraw_['from_']
withdraw_['hr'] = withdraw_.block_timestamp.str[:-9] + '00:00.000'
withdraw_['day'] = withdraw_.block_timestamp.str[:-13]
withdraw_ = withdraw_[['block_timestamp','sender','tx_id','action','prism_amount','yluna_amount','hr','day']]
withdraw_['amount_signed'] = -withdraw_.yluna_amount
withdraw_


# In[161]:


all_lps = withdraw_.append(provide_)
daily_delta_lp = all_lps.groupby('day').amount_signed.sum().reset_index()
daily_delta_lp = daily_delta_lp.sort_values(by='day')
daily_delta_lp['cumsum'] = daily_delta_lp.amount_signed.cumsum().apply(lambda x: round(x,2))
daily_delta_lp.columns = ['Time', 'Amount signed', 'Amount']
daily_delta_lp['Type'] = 'yLuna LP'
daily_delta_lp.head()


# ### Refract

# In[162]:


refract_cluna_df.head()


# In[163]:


refract_cluna_df['user'] = refract_cluna_df['from_']


# In[164]:


refract_cluna_df.shape


# In[165]:


refract_cluna_df[refract_cluna_df.user=='terra1uh37lsydrup8vqvvttd53qwj93ft9x7572g62q']


# In[166]:


refract_cluna_df_pol = refract_cluna_df[['block_timestamp','tx_id','user','0_action','0_amount']]
refract_cluna_df_pol.columns = ['block_timestamp','tx_id','user','action','amount']


# In[167]:


refract_cluna_df_pol['asset_given'] = 'cLUNA'
refract_cluna_df_pol['asset_received'] = 'yLUNA'


# In[168]:


refract_cluna_df_pol['operation'] = 'refraction'


# In[169]:


refract_cluna_df_pol['hr'] = refract_cluna_df_pol.block_timestamp.str[:-9] + '00:00.000'
refract_cluna_df_pol['day'] = refract_cluna_df_pol.block_timestamp.str[:-13]


# In[170]:


refract_cluna_df_pol['amount_signed'] = refract_cluna_df_pol.amount
refract_cluna_df_pol.head()


# ### Refract

# In[171]:


refract_df['user'] = refract_df.apply(lambda row: row.from_ if row['0_action']=='bond_split' else row.to_, axis=1)


# In[172]:


refract_df.shape


# In[173]:


refract_df[refract_df.user=='terra1uh37lsydrup8vqvvttd53qwj93ft9x7572g62q']


# In[174]:


refract_df_pol = refract_df[['block_timestamp','tx_id','user','0_action','0_amount']]
refract_df_pol.columns = ['block_timestamp','tx_id','user','action','amount']


# In[175]:


refract_df_pol['asset_given'] = refract_df_pol.apply(lambda row: 'LUNA' if row['action']=='bond_split' else 'yLUNA',axis=1)
refract_df_pol['asset_received'] = refract_df_pol.apply(lambda row: 'yLUNA' if row['action']=='bond_split' else 'LUNA',axis=1)


# In[176]:


refract_df_pol['operation'] = 'refraction'


# In[177]:


refract_df_pol[refract_df_pol.user=='terra1uh37lsydrup8vqvvttd53qwj93ft9x7572g62q']


# In[178]:


refract_df_pol['hr'] = refract_df_pol.block_timestamp.str[:-9] + '00:00.000'
refract_df_pol['day'] = refract_df_pol.block_timestamp.str[:-13]


# In[179]:


refract_df_pol.head()


# In[180]:


refract_df_pol['amount_signed'] = refract_df_pol.apply(lambda row: -row.amount 
                                                if row.action=='burn_from' else row.amount,axis=1)
refract_df_pol.head()


# In[181]:


all_refreact = refract_df_pol.append(refract_cluna_df_pol)


# In[182]:


daily_delta_rf = all_refreact.groupby('day').amount_signed.sum().reset_index()
daily_delta_rf = daily_delta_rf.sort_values(by='day')
daily_delta_rf['cumsum'] = daily_delta_rf.amount_signed.cumsum().apply(lambda x: round(x,2))
daily_delta_rf.columns = ['Time', 'Amount signed', 'Amount']
daily_delta_rf['Type'] = 'yLuna circulating'


# In[183]:


domain = ['yLuna circulating']
range_ = ['lightblue']
max_date = daily_delta_rf['Time'].max()
mars_price_chart = alt.Chart(daily_delta_rf).mark_line(point = True).encode(
    x=alt.X('Time:T',scale=alt.Scale(domain=(daily_delta_rf.Time.min(),max_date))),
    y=alt.X('Amount:Q',scale=alt.Scale(domain=(0,daily_delta_rf['Amount'].max()+100000))),
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
mars_price_chart


# ### yStaking

# In[184]:


ystaking_df.head()


# In[185]:


ystaking_df['action'] = ystaking_df.apply(lambda row: row['0_action'] if row['0_action']=='unbond' else row['1_action'],axis=1)
ystaking_df['amount'] = ystaking_df.apply(lambda row: row['0_amount'] if row['0_action']=='unbond' else row['1_amount'],axis=1)
ystaking_df['user'] = ystaking_df.apply(lambda row: row['to_'] if row.action=='unbond' else row['from_'],axis=1)
ystaking_df['asset_given'] = ystaking_df.apply(lambda row: None if row.action=='unbond' else 'yLUNA',axis=1)
ystaking_df['asset_received'] = ystaking_df.apply(lambda row: 'yLUNA' if row.action=='unbond' else None,axis=1)
ystaking_df['day'] = ystaking_df.hr.str[:-13]


# In[186]:


ystaking_df = ystaking_df[['block_timestamp','tx_id','hr','day','action','amount','user','asset_given','asset_received']]


# In[187]:


ystaking_df


# In[188]:


ystaking_df[ystaking_df.user=='terra12j6p3tausehd7495as98vql74p0f9t2ahnafsv']


# In[189]:


ystaking_df['amount_signed'] = ystaking_df.apply(lambda row: row.amount if row.action=='bond' else -row.amount,axis=1)


# In[190]:


daily_delta_stk = ystaking_df.groupby('day').amount_signed.sum().reset_index()
daily_delta_stk = daily_delta_stk.sort_values(by='day')
daily_delta_stk['cumsum'] = daily_delta_stk.amount_signed.cumsum().apply(lambda x: round(x,2))
daily_delta_stk.columns = ['Time', 'Amount signed', 'Amount']
daily_delta_stk['Type'] = 'yLuna staked'


# In[191]:


def get_max_domain_date(df, time_field, n_hours):
    if((pd.Timestamp(df[time_field].max()) - 
                pd.Timestamp(df[time_field].min())).total_seconds()/3600 < n_hours):
        max_date = (pd.Timestamp(df[time_field].min()) + pd.to_timedelta(n_hours, unit='h')).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        max_date = df[time_field].max()
    return max_date


# In[192]:


domain = ['yLuna staked']
range_ = ['#f8936d']
max_date = daily_delta_stk['Time'].max()
mars_price_chart = alt.Chart(daily_delta_stk).mark_line(point = True).encode(
    x=alt.X('Time:T',scale=alt.Scale(domain=(daily_delta_stk.Time.min(),max_date))),
    y=alt.X('Amount:Q',scale=alt.Scale(domain=(0,daily_delta_stk['Amount'].max()+100000))),
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
mars_price_chart


# ## All deltas

# In[193]:


all_deltas = daily_delta_rf.append(daily_delta_stk).append(daily_delta_lp)


# In[203]:


domain = ['yLuna staked','yLuna circulating','yLuna LP']
range_ = ['#f8936d','lightblue','green']
max_date = all_deltas['Time'].max()
mars_price_chart = alt.Chart(all_deltas).mark_line(point = True).encode(
    x=alt.X('Time:T',scale=alt.Scale(domain=(all_deltas.Time.min(),max_date))),
    y=alt.X('Amount:Q',scale=alt.Scale(domain=(0,all_deltas['Amount'].max()+100000))),
    color=alt.Color('Type:N', 
                sort=domain,
                scale=alt.Scale(domain=domain, range=range_),
                legend=alt.Legend(
                            orient='none',
                            padding=5,
                            legendY=0,
                            direction='horizontal')),
    tooltip=[alt.Tooltip('Time:T', format='%Y-%m-%d %H:%M'),'Type:N','Amount:Q']
).properties(width=800).properties(height=300).configure_view(strokeOpacity=0).interactive()
mars_price_chart


# ## Query

# In[204]:


import requests
luna_apr = float(
    requests.get('https://api.terra.dev/chart/staking-return/annualized').json(
    )[-1]['value']) * 100
et_query = requests.get(
    'https://api.extraterrestrial.money/v1/api/prices').json()
yluna_price = float(et_query['prices']['yLUNA']['price'])
luna_price = float(et_query['prices']['LUNA']['price'])
yluna_apr = luna_apr * luna_price / yluna_price


# In[205]:


yluna_apr


# In[206]:


luna_apr


# In[207]:


luna_price


# In[260]:


df = pd.DataFrame(requests.get('https://api.terra.dev/chart/staking-return/annualized').json())
df['date'] = pd.to_datetime(df['datetime'], unit='ms')
df = df[df['date'] > '2021-07-05 15:00:00']


# In[261]:


df.value = df.value.apply(float).apply(lambda x: x*100)


# In[262]:


mars_price_chart = alt.Chart(df).mark_line(point = True).encode(
    x=alt.X('date:T'),
    y=alt.X('value:Q'),
    tooltip=[alt.Tooltip('date:T', format='%Y-%m-%d %H:%M'),'value:Q']
).properties(width=800).properties(height=300).configure_view(strokeOpacity=0).interactive()
mars_price_chart


# In[ ]:




