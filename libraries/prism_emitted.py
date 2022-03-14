#!/usr/bin/env python
# coding: utf-8

# In[23]:


import pandas as pd
import altair as alt
import warnings
import datetime

warnings.filterwarnings("ignore")
alt.renderers.set_embed_options(theme='light')
pd.set_option("display.max_colwidth", 400)
pd.set_option("display.max_rows", 400)
alt.renderers.set_embed_options(theme='dark')


# In[24]:


class PrismEmittedDataProvider():
    def __init__(self):
        pass

    def load(self):
        self.calulcate_emission()
        self.dates_to_mark()
    
    def dates_to_mark(self):
        dates_to_mark = []
        start_farm = datetime.date(2022, 3, 5)
        shift = 3
        date_ = start_farm+datetime.timedelta(days=int(365/100*10))
        dates_to_mark.append((8000000,'10%',date_+datetime.timedelta(days=-shift),date_))
        date_ = start_farm+datetime.timedelta(days=int(365/100*30))
        dates_to_mark.append((8000000,'30%',date_+datetime.timedelta(days=-shift),date_))
        date_ = start_farm+datetime.timedelta(days=int(365/100*50))
        dates_to_mark.append((8000000,'50%',date_+datetime.timedelta(days=-shift),date_))
        date_ = start_farm+datetime.timedelta(days=int(365))
        dates_to_mark.append((8000000,'100%',date_+datetime.timedelta(days=-shift),date_))
        dates_to_mark = pd.DataFrame(dates_to_mark,columns=['height','text','text_date','Date'])
        dates_to_mark.Date = dates_to_mark.Date.apply(str)
        dates_to_mark.text_date = dates_to_mark.text_date.apply(str)
        dates_to_mark = dates_to_mark.merge(self.prism_emitted[['Date','Total Prism']],on='Date')
        self.dates_to_mark = dates_to_mark
    
    def calulcate_emission(self):
        data = []
        start_farm = datetime.date(2022, 3, 5)
        base_tot = 104000000
        boost_tot = 26000000
        tot = base_tot+boost_tot
        for i in range(1,366):
            data.append((start_farm+datetime.timedelta(days=i), tot/365*i, base_tot/365*i, boost_tot/365*i))
        prism_emitted = pd.DataFrame(data, columns=['Date','Total Prism','Normal','Boost',])
        prism_emitted.Date = prism_emitted.Date.apply(str)
        self.prism_emitted = prism_emitted
        
        normal = prism_emitted[['Date','Normal']]
        normal.columns = ['Date','Amount']
        normal['Type'] = 'Normal'
        boost = prism_emitted[['Date','Boost']]
        boost.columns = ['Date','Amount']
        boost['Type'] = 'Boost'
        df = normal.append(boost)
        self.prism_emitted_so_far = df[df.Date<str(datetime.datetime.today().date())]


# In[25]:


pe_dp = PrismEmittedDataProvider()


# In[26]:


class PrismEmittedChartProvider:
    def __init__(self):
        pass
    
    def prism_emitted_trend_line(self, prism_emitted):
        cum_ust_chart = alt.Chart(prism_emitted).mark_line(strokeDash=[6,6]).encode(
            x=alt.X('Date:T'),
            y='Total Prism'
        )
        return cum_ust_chart
    def prism_emitted_so_far(self, prism_emitted_so_far):
        chart = alt.Chart(prism_emitted_so_far).mark_area().encode(
            x=alt.X('Date:T', scale=alt.Scale(domain=(prism_emitted_so_far.Date.min(),'2022-04-15'))),
            y=alt.Y('Amount:Q',scale=alt.Scale(domain=(0,16000000))),
            color=alt.Color('Type:N',
                        legend=alt.Legend(
                                    orient='none',
                                    padding=5,
                                    legendY=0,
                                    direction='vertical')),
            tooltip=['Type:N','Amount:Q']
        )
        return chart
    
    def dates_to_mark(self, dates_to_mark):
        c2 = alt.Chart(dates_to_mark).mark_rule(color='#e45756').encode(
            x='Date'+':T'
        )

        c3 = alt.Chart(dates_to_mark).mark_text(
            color='#e45756',
            angle=0
        ).encode(
            x=alt.X('text_date'+':T',axis=alt.Axis(title='')),
            y=alt.Y('height',axis=alt.Axis(title='Prism Emitted')),
            text='text'
        )
        return c2, c3
    
    def prism_emitted_chart(self, prism_emitted, prism_emitted_so_far,dates_to_mark):
        trend_line = self.prism_emitted_trend_line(prism_emitted)
        so_far = self.prism_emitted_so_far(prism_emitted_so_far)
        marks, lines = self.dates_to_mark(dates_to_mark)
        chart =  (so_far+trend_line+marks+lines)                .configure_mark(
                    color='#ffffff'
                ).properties(width=900).configure_view(strokeOpacity=0).interactive()
        return chart
