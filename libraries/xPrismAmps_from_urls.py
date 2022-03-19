#!/usr/bin/env python
# coding: utf-8

# In[143]:


import pandas as pd
import altair as alt
import warnings
import requests


# In[149]:


class xPrismAMPsDP:
    
    def __init__(self, claim):
        self.xprism_bal_url = """https://terra-api.daic.capital/api/tx/GetRichlistByTokenContract?apiKey=vAp6ysmAXH470YcphYxv&contract_address=terra1042wzrwg2uk6jqxjm34ysqquyr9esdgm5qyswz"""
        self.amps_hash = '91ea3cfb-2456-4ca9-b773-be6074835316'
        self.claim = claim
        
    def load(self):
        xprism = requests.get(self.xprism_bal_url).json()['result']['holders']
        df = pd.DataFrame(xprism.values(),xprism.keys()).reset_index()
        df.columns = ['user','xprism']
        self.xprism_bal = df
        
        amps = self.claim(self.amps_hash)
        amps.columns = [c.lower() for c in amps.columns]
        amps = amps.merge(self.xprism_bal, on='user', how='outer').fillna(0)
        amps['pledge_net'] = amps.pledge_amount - amps.unpledge_amount
        amps['tot_xprism'] = amps['pledge_net']+amps.xprism
        self.amps = amps
    
    def parse(self):
        df = (self.amps.apply(lambda row: row['pledge_net']/row.tot_xprism if row.tot_xprism > 0 else 1,axis=1)*10).apply(int).value_counts()
        df = df.reset_index()
        df.columns = ['Percentage of xPRISM in AMPs','Number of users']
        df['Number of users'] = df['Number of users'].apply(lambda x: round(x,2))
        df['Percentage of xPRISM in AMPs'] = df['Percentage of xPRISM in AMPs'].apply(lambda x: round(x,2))
        m = {}
        for i in range(10):
            m[i]=f"{(i)*10}%-{(i+1)*10}%"
        m[10] = '90%-100%'
        df['Percentage of xPRISM in AMPs'] = df['Percentage of xPRISM in AMPs'].map(m)
        self.perc_amps_n_user = df


# In[145]:


def claim(claim_hash):
    df = pd.read_json(
            f"https://api.flipsidecrypto.com/api/v2/queries/{claim_hash}/data/latest",
            convert_dates=["BLOCK_TIMESTAMP"])
    df.columns = [c.lower() for c in df.columns]
    return df



# In[150]:


class xPrismAmpsChart:
    
    def chart(perc_amps_n_user):
        return alt.Chart(perc_amps_n_user).mark_bar(color='#ccf4ed').encode(
            x = alt.X('Percentage of xPRISM in AMPs:N', axis=alt.Axis(tickCount=10, labelAngle=0, tickBand = 'center')),
            y='Number of users',
            tooltip= ['Percentage of xPRISM in AMPs', 'Number of users']
        ).properties(width=600).configure_view(strokeOpacity=0)



