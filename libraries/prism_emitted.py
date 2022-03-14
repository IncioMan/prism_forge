#!/usr/bin/env python
# coding: utf-8

# In[60]:


import pandas as pd
import altair as alt
import warnings
import datetime

warnings.filterwarnings("ignore")
alt.renderers.set_embed_options(theme='light')
pd.set_option("display.max_colwidth", 400)
pd.set_option("display.max_rows", 400)
alt.renderers.set_embed_options(theme='dark')


# In[187]:


class PrismEmittedDataProvider():
    def __init__(self):
        self.calculcate_emission()
        self.dates_to_mark()
    
    def dates_to_mark(self):
        dates_to_mark = []
        start_farm = datetime.date(2022, 3, 5)
        shift = 1
        #date_ = start_farm+datetime.timedelta(days=int(365/100*10))
        #dates_to_mark.append((8000000,'10%',date_+datetime.timedelta(days=-shift),date_))
        date_ = start_farm+datetime.timedelta(days=int(365/100*20))
        dates_to_mark.append((8000000,'20%',date_+datetime.timedelta(days=-shift),date_))
        date_ = start_farm+datetime.timedelta(days=int(365/100*30))
        dates_to_mark.append((8000000,'30%',date_+datetime.timedelta(days=-shift),date_))
        date_ = start_farm+datetime.timedelta(days=int(365/100*50))
        dates_to_mark.append((8000000,'50%',date_+datetime.timedelta(days=-shift),date_))
        date_ = start_farm+datetime.timedelta(days=int(365))
        dates_to_mark.append((8000000,'100%',date_+datetime.timedelta(days=-shift),date_))
        date_ = datetime.datetime.today().date()
        perc = round(self.prism_emitted[self.prism_emitted.Date==str(date_)]['Total Prism'].values[0]/130000000,2)
        dates_to_mark.append((8000000,f'{perc}%',date_+datetime.timedelta(days=-shift),date_))
        dates_to_mark = pd.DataFrame(dates_to_mark,columns=['height','text','text_date','Date'])
        dates_to_mark.Date = dates_to_mark.Date.apply(str)
        dates_to_mark.text_date = dates_to_mark.text_date.apply(str)
        dates_to_mark = dates_to_mark.merge(self.prism_emitted[['Date','Total Prism']],on='Date')
        self.dates_to_mark = dates_to_mark
        extra_dates_to_mark = []
        date_ = start_farm+datetime.timedelta(days=30)
        extra_dates_to_mark.append((17000000,'Unlock starts',date_+datetime.timedelta(days=-shift),date_))
        extra_dates_to_mark = pd.DataFrame(extra_dates_to_mark,columns=['height','text','text_date','Date'])
        extra_dates_to_mark.Date = extra_dates_to_mark.Date.apply(str)
        extra_dates_to_mark.text_date = extra_dates_to_mark.text_date.apply(str)
        self.extra_dates_to_mark = extra_dates_to_mark
    
    def calculcate_emission(self):
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
        self.prism_emitted_so_far = df[df.Date<=str(datetime.datetime.today().date())]


# In[188]:


pe_dp = PrismEmittedDataProvider()


# In[189]:


class PrismEmittedChartProvider:
    def __init__(self):
        pass
    
    def prism_emitted_trend_line(self, prism_emitted, final_date):
        if(final_date):
            prism_emitted = prism_emitted[prism_emitted.Date<=final_date]
        cum_ust_chart = alt.Chart(prism_emitted).mark_line(strokeDash=[6,6]).encode(
            x=alt.X('Date:T'),
            y='Total Prism'
        )
        return cum_ust_chart
    def prism_emitted_so_far(self, prism_emitted_so_far, final_date):
        if(final_date):
            prism_emitted_so_far = prism_emitted_so_far[prism_emitted_so_far.Date<=final_date]
        prism_emitted_so_far['Amount emitted'] = prism_emitted_so_far.Amount.apply(lambda x: str(round(x/1000000,2))+'M')
        chart = alt.Chart(prism_emitted_so_far).mark_area().encode(
            x=alt.X('Date:T'),
            y=alt.Y('Amount:Q'),
            color=alt.Color('Type:N',
                        legend=alt.Legend(
                                    orient='none',
                                    padding=5,
                                    legendY=0,
                                    direction='vertical')),
            tooltip=['Date:T', 'Type:N','Amount emitted']
        )
        return chart
    
    def dates_to_mark(self, dates_to_mark, extra_dates_to_mark, final_date):
        if(final_date):
            dates_to_mark = dates_to_mark[dates_to_mark.Date <= final_date]
        c2 = alt.Chart(dates_to_mark).mark_rule(color='#e45756').encode(
            x='Date'+':T'
        )

        c3 = alt.Chart(dates_to_mark).mark_text(
            color='#e45756',
            angle=0
        ).encode(
            x=alt.X('text_date'+':T',axis=alt.Axis(title='')),
            y=alt.Y('height'),
            text='text'
        )
        
        c4 = alt.Chart(extra_dates_to_mark).mark_rule(color='#a6cfe3').encode(
            x='Date'+':T'
        )

        c5 = alt.Chart(extra_dates_to_mark).mark_text(
            color='#a6cfe3',
            angle=270
        ).encode(
            x=alt.X('text_date'+':T',axis=alt.Axis(title='')),
            y=alt.Y('height',axis=alt.Axis(title='Prism Emitted')),
            text='text'
        )
        return c2, c3, c4, c5
    
    def prism_emitted_chart(self, prism_emitted, prism_emitted_so_far,dates_to_mark, extra_dates_to_mark, final_date=None):
        trend_line = self.prism_emitted_trend_line(prism_emitted, final_date)
        so_far = self.prism_emitted_so_far(prism_emitted_so_far, final_date)
        marks, lines, marks2, lines2 = self.dates_to_mark(dates_to_mark, extra_dates_to_mark, final_date)
        return (so_far+trend_line+marks+lines+marks2+lines2)                .configure_mark(
                    color='#ffffff'
                ).configure_axis(grid=False).configure_view(strokeOpacity=0)


# In[194]:


cp = PrismEmittedChartProvider()
cp.prism_emitted_chart(pe_dp.prism_emitted, pe_dp.prism_emitted_so_far, 
                       pe_dp.dates_to_mark, pe_dp.extra_dates_to_mark, '2022-05-25')


# In[ ]:





# In[ ]:




