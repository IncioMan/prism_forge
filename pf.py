import streamlit as st
import pandas as pd
import altair as alt
from charts import ChartProvider
import requests
import datetime
from PIL import Image
from libraries.prism_analytics import DataProvider, ChartProvider, LPDataProvider, SwapsDataProvider, RefractDataProvider, CollectorDataProvider, YLunaStakingDataProvider
from libraries.prism_emitted import PrismEmittedChartProvider, PrismEmittedDataProvider

st.set_page_config(page_title="Prism Forge - Analytics",\
        page_icon=Image.open(requests.get('https://raw.githubusercontent.com/IncioMan/prism_forge/master/images/xPRISM.png',stream=True).raw),\
        layout='wide')

###

@st.cache(ttl=3000, show_spinner=False, allow_output_mutation=True)
def claim(claim_hash, cols_claim=[]):
    df_claim = pd.read_json(
        f"https://api.flipsidecrypto.com/api/v2/queries/{claim_hash}/data/latest",
        convert_dates=["BLOCK_TIMESTAMP"],
    )
    return df_claim

@st.cache(ttl=3000, show_spinner=False, allow_output_mutation=True)
def get_url(url):
    return pd.read_csv(url, index_col=0)
    
cp = ChartProvider()
ystake_dp = YLunaStakingDataProvider(claim,get_url,'./data')
refract_dp = RefractDataProvider(claim,get_url,'./data')
swaps_dp = SwapsDataProvider(claim,get_url,'./data')
lp_dp = LPDataProvider(claim,get_url,'./data')
collector_dp = CollectorDataProvider(claim,get_url,'./data')
ydp = DataProvider('yLuna')
pdp = DataProvider('pLuna')
pe_dp = PrismEmittedDataProvider()
pe_cp = PrismEmittedChartProvider()

@st.cache(ttl=3000, show_spinner=False, allow_output_mutation=True)
def get_data(pe_dp, ystake_dp, refract_dp, swaps_dp, lp_dp, collector_dp, ydp, pdp, to_csv=False):
    print("{} - Loading data...".format(str(datetime.datetime.now()).split('.')[0]), flush=True)
    ystake_dp.load()
    refract_dp.load()
    swaps_dp.load()
    lp_dp.load()
    collector_dp.load()
    
    if(to_csv):    
        ystake_dp.write_to_csv()
        refract_dp.write_to_csv()
        swaps_dp.write_to_csv()
        lp_dp.write_to_csv()
        collector_dp.write_to_csv()
    
    ystake_dp.parse()
    refract_dp.parse()
    swaps_dp.parse()
    lp_dp.parse()
    collector_dp.parse(lp_dp.withdraw_, lp_dp.provide_, swaps_dp.swaps_df_all)

    ydp.lp_delta(lp_dp.withdraw_[lp_dp.withdraw_.asset=='yLuna'],
            lp_dp.provide_[lp_dp.provide_.asset=='yLuna'], 
            swaps_dp.yluna_swaps, collector_dp.collector_pyluna[collector_dp.collector_pyluna.asset=='yLuna'])
    ydp.stk_delta(ystake_dp.ystaking_df)
    ydp.stk_farm_delta(ystake_dp.ystaking_farm_df)
    ydp.refact_delta(refract_dp.all_refreact)
    ydp.all_delta()
    ydp.unused_asset(ydp.all_deltas)

    pdp.lp_delta(lp_dp.withdraw_[lp_dp.withdraw_.asset=='pLuna'],
            lp_dp.provide_[lp_dp.provide_.asset=='pLuna'], 
            swaps_dp.yluna_swaps, collector_dp.collector_pyluna[collector_dp.collector_pyluna.asset=='pLuna'])
    pdp.refact_delta(refract_dp.all_refreact)
    pdp.all_delta()
    pdp.unused_asset(pdp.all_deltas)
    return pe_dp.prism_emitted, pe_dp.prism_emitted_so_far, pe_dp.dates_to_mark,\
           pdp.dates_to_mark, pdp.asset_used, pdp.asset_unused, ydp.dates_to_mark,\
           ydp.asset_used, ydp.asset_unused

pe_dp_prism_emitted, pe_dp_prism_emitted_so_far, pe_dp_dates_to_mark,\
pdp_dates_to_mark, pdp_asset_used, pdp_asset_unused, ydp_dates_to_mark,\
ydp_asset_used, ydp_asset_unused = get_data(pe_dp, ystake_dp, refract_dp, 
                                            swaps_dp, lp_dp, collector_dp, ydp, pdp)

###
###
all_deltas = ydp_asset_used.append(ydp_asset_unused)
all_deltas = ydp.fill_date_gaps(all_deltas, ['2022-02-11','2022-02-12','2022-02-13'])
c1 = cp.get_yluna_time_area_chart(all_deltas, 
               alt.Scale(scheme='set2'),
               min_date = all_deltas.Time.min(),
               max_date = all_deltas.Time.max(),
               top_padding = 1500000
        )

c2 = alt.Chart(ydp_dates_to_mark).mark_rule(color='#e45756').encode(
    x=alt.X('date'+':T',axis=alt.Axis(labels=False,title=''))
)

c3 = alt.Chart(ydp_dates_to_mark).mark_text(
    color='#e45756',
    angle=0
).encode(
    x=alt.X('text_date'+':T',axis=alt.Axis(labels=False,title='')),
    y='height',
    text='text'
)

yluna_chart = (c1 + c2 + c3).properties(height=400).configure_view(strokeOpacity=0)

all_deltas = pdp_asset_used.append(pdp_asset_unused)
all_deltas = pdp.fill_date_gaps(all_deltas, ['2022-02-11','2022-02-12','2022-02-13'])
c1 = cp.get_yluna_time_area_chart(all_deltas, 
               alt.Scale(scheme='set2'),
               min_date = all_deltas.Time.min(),
               max_date = all_deltas.Time.max(),
               top_padding = 1500000
        )

c2 = alt.Chart(pdp_dates_to_mark).mark_rule(color='#e45756').encode(
    x=alt.X('date'+':T',axis=alt.Axis(labels=False,title=''))
)

c3 = alt.Chart(pdp_dates_to_mark).mark_text(
    color='#e45756',
    angle=0
).encode(
    x=alt.X('text_date'+':T',axis=alt.Axis(labels=False,title='')),
    y='height',
    text='text'
)

pluna_chart = (c1 + c2 + c3).properties(height=400).configure_view(strokeOpacity=0)

prism_emitted_chart = pe_cp.prism_emitted_chart(pe_dp.prism_emitted, pe_dp.prism_emitted_so_far, 
                       pe_dp.dates_to_mark, pe_dp.extra_dates_to_mark, '2022-05-25')

st.altair_chart(prism_emitted_chart, use_container_width=True)

st.subheader('yLuna usage')
st.markdown("""How is yLuna used? Staked or in the liquidity pool? What happened over time?""")
st.altair_chart(yluna_chart, use_container_width=True)
st.subheader('pLuna usage')
st.markdown("""How much pLuna is put to use? How much is idle? What happened over time?""")
st.altair_chart(pluna_chart, use_container_width=True)
st.markdown("""
<style>
    @media (min-width:640px) {
        .block-container {
            padding-left: 5rem;
            padding-right: 5rem;
        }
    }
    @media (min-width:800px) {
        .block-container {
            padding-left: 10rem;
            padding-right: 10rem;
        }
    }
    .block-container
    {
        padding-bottom: 1rem;
        padding-top: 5rem;
    }
</style>
""", unsafe_allow_html=True)
st.markdown("""
<style>
.terminated {
    margin-left: 10px;
    width: 10px;
    height: 10px;
    display: inline-block;
    border: 1px solid red;
    background-color: red;
    border-radius: 100%;
    opacity: 0.8;
}

.idle {
    margin-left: 10px;
    width: 10px;
    height: 10px;
    display: inline-block;
    border: 1px solid grey;
    background-color: grey;
    border-radius: 100%;
    opacity: 0.8;
}

.blink_me {
    margin-left: 10px;
    animation: blinker 2s linear infinite;
    width: 10px;
    height: 10px;
    display: inline-block;
    border: 1px solid green;
    background-color: green;
    border-radius: 100%;
    }
    @keyframes blinker {
    50% {
        opacity: 0;
    }
}

@media (min-width:800px) {
    .css-12w0qpk {
        padding-left: 30px;
    }
}
</style>
""", unsafe_allow_html=True)
hide_streamlit_style = """
                        <style>
                        #MainMenu {visibility: hidden;}
                        footer {visibility: hidden;}
                        </style>
                        """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)
