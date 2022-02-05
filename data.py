import pandas as pd
from constants import cols_dict
import requests
import random

class DataProvider:
    
    def load_data_p1(self):
        self.user_stats_df = self.claim(self.user_stats, self.cols_claim)
        self.deposit_balance_df = self.claim(self.deposit_balance, self.cols_claim)
        self.deposits_bucket_df = self.claim(self.deposits_bucket, self.cols_claim)
        self.wallet_age_df = self.claim(self.wallet_age, self.cols_claim)
        self.hourly_stats_df = self.claim(self.hourly_stats, self.cols_claim)
        self.prev_launches_df = self.claim(self.prev_launches, self.cols_claim)

        self.user_stats_df['DEPOSIT_NET'] = self.user_stats_df.DEPOSIT_AMOUNT - self.user_stats_df.WITHDRAWN_AMOUNT
        self.user_stats_df['N_TXS'] = self.user_stats_df.DEPOSIT_TXS + self.user_stats_df.WITHDRAW_TXS
        self.top_depositors = self.user_stats_df.sort_values(by='DEPOSIT_NET', ascending=False).head(5)[['SENDER','DEPOSIT_NET']]\
                    .set_index('SENDER').rename(columns=cols_dict)

        self.hourly_new_users_df = self.claim(self.hourly_new_users, self.cols_claim)
        self.hourly_new_users_df['cumsum_new_users'] = self.hourly_new_users_df.sort_values(by='TIME').NEW_USERS.cumsum()
        self.hourly_new_users_df['Hour'] = self.hourly_new_users_df['TIME']
        df = self.hourly_new_users_df.sort_values(by='TIME')
        index = df.index
        if(len(index)>1):
            self.n_users = df.loc[index[-1]].cumsum_new_users
            i = -2
            self.next_last_users = df.loc[index[i]].cumsum_new_users
        else:
            self.n_users = 0
            self.next_last_users = 0
        self.hourly_new_users_df = self.hourly_new_users_df.rename(columns=cols_dict)



        self.wallet_age_df = self.wallet_age_df.rename(columns=cols_dict)

        self.hourly_stats_df['cumsum_ust'] = self.hourly_stats_df.sort_values(by='HR').NET_AMOUNT.cumsum()
        self.hourly_stats_df['cumsum_txs'] = self.hourly_stats_df.sort_values(by='HR').TOT_TXS.cumsum()
        self.hourly_stats_df['Hour'] = self.hourly_stats_df['HR']
        df = self.hourly_stats_df.sort_values(by='HR')
        d = requests.get("http://fcd.terra.dev/v1/bank/terra1angxk38zehp0k09m0wqrrxf0r3ces6qjj432l8").json()
        self.tot_deposits = int(d['balance'][0]['available'])/1000000
        index = df.index
        if(len(index)>1):
            self.n_txs = df.loc[index[-1]].cumsum_txs
            i = -2
            self.next_last_ust = df.loc[index[i]].cumsum_ust
            self.next_last_txs = df.loc[index[i]].cumsum_txs
        else:
            self.n_txs = 0
            i = 0
            self.next_last_ust = 0
            self.next_last_txs = 0
        self.hourly_stats_df = self.hourly_stats_df.rename(columns=cols_dict)

        self.prev_launches_df = self.prev_launches_df.rename(columns=cols_dict)

        self.deposit_balance_df = self.deposit_balance_df.rename(columns=cols_dict)
        self.deposits_bucket_df['bucket_name']= self.deposits_bucket_df.BUCKET.map({0:'-$0',1:'$0-$10',2:'$10-$100',3:'$100-$1k',4:'$1k-$10k',
                                        5:'$10k-$100k',6:'$100k-$1m',7:'$1m-'})
        self.deposits_bucket_df.sort_values(by='BUCKET')

        self.dates_to_mark = pd.DataFrame([
            ['2021-03-04', '2021-03-11',15,'Anchor launch'],
            ['2021-09-24', '2021-10-01',15,'Columbus 5'],
            ['2021-12-12', '2021-12-19',15,'Astroport launch'], 
            ['2022-01-24', '2022-02-01',15,'Prism launch']], 
            columns=['text_date','date','height','text']
        )
    
    def load_data_p2(self):
        self.user_stats_df = self.claim(self.user_stats, self.cols_claim)
        self.user_stats_df.columns = [c.lower() for c in self.user_stats_df.columns]
        self.p2_users_df = self.claim(self.p2_users, self.cols_claim)
        self.p2_users_df.columns = [c.lower() for c in self.p2_users_df.columns]
        self.p2_hourly_df = self.claim(self.p2_hourly, self.cols_claim)
        self.p2_hourly_df = self.p2_hourly_df.sort_values(by='HR')
        self.p2_hourly_df['cumsum_with'] = self.p2_hourly_df.sort_values(by='HR').WITH_AMOUNT.cumsum()
        

        self.with_phase1=self.get_url('https://raw.githubusercontent.com/IncioMan/prism_forge/p22/data/with_phase1.csv')
        self.users_with = self.p2_users_df.merge(self.with_phase1,on='sender')[['sender','net_deposited_amount','deposit','withdrawable_amount']]
        self.users_with= self.users_with.rename(columns={'net_deposited_amount':'deposited_p1'})
        self.users_with['WITHDRAWN_AMOUNT_PHASE2'] = self.users_with.deposited_p1-self.users_with.deposit
        self.users_with['WITHDRAWN_AMOUNT_PHASE2'] = self.users_with['WITHDRAWN_AMOUNT_PHASE2'].apply(lambda x: x if x >0 else 0)
        self.users_with['has_withdrawn_p2'] = (self.users_with['WITHDRAWN_AMOUNT_PHASE2'])>1


        self.tot_with_ust = self.users_with['WITHDRAWN_AMOUNT_PHASE2'].sum()
        self.tot_ust_p1 = self.users_with['deposited_p1'].sum()
        self.p2_hourly_df['ust_left'] = self.tot_ust_p1 - self.p2_hourly_df['cumsum_with']
        self.tot_net_ust = self.users_with.deposit.sum()
        self.perc_with_p2 = self.tot_with_ust/self.tot_ust_p1 * 100
        self.curr_price = self.tot_net_ust/70000000
        self.p_users_with_p2 = (self.users_with.has_withdrawn_p2.sum()/len(self.p2_users_df))*100
        self.p2_users_df['max_with_hour'] = '2022-02-03 10:00:00.000'
        
        self.ust_df = pd.DataFrame([[self.tot_with_ust,self.tot_ust_p1-self.tot_with_ust],['Withdrawn','Still deposited']]).T
        self.ust_df.columns = ['UST','Type']

        self.left_to_with = self.users_with.withdrawable_amount.sum()
        self.floor_price = (self.tot_net_ust - self.left_to_with)/70000000
        

        self.with_users_df = self.users_with[self.users_with.has_withdrawn_p2&self.users_with['deposited_p1']>0]
        self.with_users_df['perc_withdrawn'] = round(self.with_users_df[self.with_users_df.has_withdrawn_p2]['WITHDRAWN_AMOUNT_PHASE2'],3)/self.with_users_df['deposited_p1']*10
        self.with_users_df['perc_withdrawn_cat'] = (self.with_users_df['perc_withdrawn']).apply(lambda x: int(x) if int(x) < 10 else 9)
        df2 = self.with_users_df.groupby('perc_withdrawn_cat').sender.count()
        perc_cat = list(range(0,10))
        cat = pd.DataFrame([0]*10,perc_cat)
        df3 = cat.join(df2,how='outer')
        df3.index =  ['0%-10%','10%-20%','20%-30%','30%-40%','40%-50%','50%-60%','60%-70%','70%-80%','80%-90%','90%-100%']
        df3 = df3.sender.fillna(0).reset_index()
        self.with_perc_buckets=df3.rename(columns={'index':'PERC_WITHDRAWN','sender':'TOT_USERS'})
        
        self.with_users_df['perc_withdrawn_cat_old'] = self.with_users_df['perc_withdrawn_cat']
        self.with_users_df['perc_withdrawn_cat'] = self.with_users_df['perc_withdrawn_cat'].apply(lambda x: int(int(x*10/5)+2))
        self.with_users_df['DEP_CAT'] = (self.with_users_df['deposited_p1']/20000).apply(int)
        df = self.with_users_df.groupby(['DEP_CAT','perc_withdrawn_cat']).sender.count()
        df = df.reset_index()

        df1 = pd.DataFrame([list(range(1,21)),
                            ["0%-5%",	"5%-10%",	"10%-15%",	"15%-20%",	"20%-25%",	"25%-30%",	"30%-35%",	
        "35%-40%",	"40%-45%",	"45%-50%",	"50%-55%",	"55%-60%",	"60%-65%",	
        "65%-70%",	"70%-75%",	"75%-80%",	"80%-85%",	"85%-90%",	"90%-95%",	"95%-100%"]]).T
        df2 = pd.DataFrame([list(range(0,29)),
                            ["0-20k", "20-40k", "40-60k", "60-80k", "80-100k", "100-120k", "120-140k", "140-160k", 
                            "160-180k", "180-200k","200-220k", "220-240k", "240-260k", "260-280k", "280-300k", "300-320k",
                            "320-340k",
                            "340-360k", 
                            "360-380k", "380-400k", "400-420k", "420-440k", "440-460k", "460-480k", "480-500k", "500-520k","520-540k",
                            "540-560k","560-580k"]]).T
        df2['fake'] = 0
        df1['fake'] = 0
        heatmap_val = df2.merge(df1,on='fake').drop('fake',1)
        heatmap_val.columns = ['DEP_CAT','DEP_CAT_label','perc_withdrawn_cat','perc_withdrawn_cat_label']
        df = heatmap_val.merge(df, on=['perc_withdrawn_cat', 'DEP_CAT'], how='left').fillna(0)
        self.heatmap_data_df = df.sort_values(by=['perc_withdrawn_cat', 'DEP_CAT'], ascending=[False,True])

    def __init__(self, claim, get_url=None):
        self.claim = claim
        self.get_url = get_url


        self.user_stats = '499224b4-30a6-43d7-80b9-3a019cbb1d3d'
        self.deposits_bucket = 'b4953cda-a874-43fa-b78d-ceb0c1bfc3cf'
        self.deposit_balance = '9e2e9587-0850-466a-8a59-4dda2e8337f3'
        self.hourly_new_users = '65179a1e-fd70-43eb-a9e4-ce14b716c928'
        self.wallet_age = '5b7983de-8596-42de-a997-767754746b71'
        self.hourly_stats = '520fb3b6-a968-4742-bf0a-31cbb67b6b05'
        self.prev_launches = '4eac9ed8-be31-4cf4-9bbe-2a0776d224ad'
        ###
        self.p2_users = '07f87a6f-5bc6-49f3-a1bb-8e7d545ba95a'
        self.p2_hourly = 'de5232e1-5d57-4232-900d-1d09d2c9438e'

        self.cols_claim = {
            self.user_stats : ['DEPOSIT_AMOUNT', 'DEPOSIT_TXS', 'SENDER', 'WITHDRAWN_AMOUNT',
            'WITHDRAW_TXS'],
            self.deposits_bucket : ['BUCKET', 'N_USERS'],
            self.prev_launches : ['PARTICIPANTS', 'PARTICIPATE_TYPE', 'TYPE'],
            self.hourly_stats : ['DEPOSIT_AMOUNT', 'DEPOSIT_TX', 'DEP_USERS', 'HR', 'NET_AMOUNT',
            'TOT_TXS', 'TOT_USERS', 'WITH_AMOUNT', 'WITH_TX', 'WITH_USERS'],
            self.wallet_age : ['ADDRESS_COUNT', 'MIN_DATE'],
            self.deposit_balance: ['AMOUNT', 'AVG_BALANCE_USD', 'MAX_BALANCE_USD', 'N_TXS', 'SENDER'],
            self.hourly_new_users: ['NEW_USERS', 'TIME'],
            self.p2_users: ["DEPOSIT_AMOUNT","HR","NET_DEPOSITED_AMOUNT","SENDER","WITHDRAWN_AMOUNT","WITHDRAWN_AMOUNT_PHASE2"],
            self.p2_hourly: ['HR', 'WITH_AMOUNT', 'WITH_TX', 'WITH_USERS']
        }