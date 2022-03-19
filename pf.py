import streamlit as st
import pandas as pd
import altair as alt
from charts import ChartProvider
import requests
import datetime
from PIL import Image
from libraries.prism_analytics import DataProvider, ChartProvider, LPDataProvider, SwapsDataProvider, RefractDataProvider, CollectorDataProvider, YLunaStakingDataProvider
from libraries.prism_emitted import PrismEmittedChartProvider, PrismEmittedDataProvider
from libraries.xPrismAmps_from_urls import xPrismAmpsChart, xPrismAMPsDP

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
xprism_amps_dp = xPrismAMPsDP(claim)
ydp = DataProvider('yLuna')
pdp = DataProvider('pLuna')
pe_dp = PrismEmittedDataProvider()
pe_cp = PrismEmittedChartProvider()

@st.cache(ttl=3000, show_spinner=False, allow_output_mutation=True)
def get_data(pe_dp, ystake_dp, refract_dp, swaps_dp, lp_dp, collector_dp, ydp, pdp, xprism_amps_dp, to_csv=False):
    print("{} - Loading data: ystake_dp...".format(str(datetime.datetime.now()).split('.')[0]), flush=True)
    ystake_dp.load()
    print("{} - Loading data: refract_dp...".format(str(datetime.datetime.now()).split('.')[0]), flush=True)
    refract_dp.load()
    print("{} - Loading data: swaps_dp...".format(str(datetime.datetime.now()).split('.')[0]), flush=True)
    swaps_dp.load()
    print("{} - Loading data: lp_dp...".format(str(datetime.datetime.now()).split('.')[0]), flush=True)
    lp_dp.load()
    print("{} - Loading data: collector_dp...".format(str(datetime.datetime.now()).split('.')[0]), flush=True)
    collector_dp.load()
    print("{} - Loading data: xprism_amps_dp...".format(str(datetime.datetime.now()).split('.')[0]), flush=True)
    xprism_amps_dp.load()
    print("{} - Data Loaded...".format(str(datetime.datetime.now()).split('.')[0]), flush=True)
      
    if(to_csv):    
        ystake_dp.write_to_csv()
        refract_dp.write_to_csv()
        swaps_dp.write_to_csv()
        lp_dp.write_to_csv()
        collector_dp.write_to_csv()
    
    print("{} - Parsing data...".format(str(datetime.datetime.now()).split('.')[0]), flush=True) 
    ystake_dp.parse()
    refract_dp.parse()
    swaps_dp.parse()
    lp_dp.parse()
    collector_dp.parse(lp_dp.withdraw_, lp_dp.provide_, swaps_dp.swaps_df_all)
    xprism_amps_dp.parse()
    print("{} - Data parsed...".format(str(datetime.datetime.now()).split('.')[0]), flush=True)

    ydp.lp_delta(lp_dp.withdraw_[lp_dp.withdraw_.asset=='yLuna'],
            lp_dp.provide_[lp_dp.provide_.asset=='yLuna'], 
            swaps_dp.yluna_swaps, collector_dp.collector_pyluna[collector_dp.collector_pyluna.asset=='yLuna'])
    ydp.stk_delta(ystake_dp.ystaking_df)
    ydp.stk_farm_delta(ystake_dp.ystaking_farm_df)
    ydp.refact_delta(refract_dp.all_refreact)
    ydp.all_delta()
    ydp.all_deltas = ydp.fill_date_gaps(ydp.all_deltas)
    ydp.unused_asset(ydp.all_deltas)

    pdp.lp_delta(lp_dp.withdraw_[lp_dp.withdraw_.asset=='pLuna'],
            lp_dp.provide_[lp_dp.provide_.asset=='pLuna'], 
            swaps_dp.yluna_swaps, collector_dp.collector_pyluna[collector_dp.collector_pyluna.asset=='pLuna'])
    pdp.refact_delta(refract_dp.all_refreact)
    pdp.all_delta()
    pdp.all_deltas = pdp.fill_date_gaps(pdp.all_deltas)
    pdp.unused_asset(pdp.all_deltas)
    return pe_dp.prism_emitted, pe_dp.prism_emitted_so_far, pe_dp.dates_to_mark,\
           pdp.dates_to_mark, pdp.asset_used, pdp.asset_unused, ydp.dates_to_mark,\
           ydp.asset_used, ydp.asset_unused, refract_dp.all_refreact, xprism_amps_dp.perc_amps_n_user

pe_dp_prism_emitted, pe_dp_prism_emitted_so_far, pe_dp_dates_to_mark,\
pdp_dates_to_mark, pdp_asset_used, pdp_asset_unused, ydp_dates_to_mark,\
ydp_asset_used, ydp_asset_unused, all_refracts, perc_amps_n_user = get_data(pe_dp, ystake_dp, refract_dp, 
                                            swaps_dp, lp_dp, collector_dp, ydp, pdp, xprism_amps_dp)

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
    x=alt.X('date'+':T')
)

c3 = alt.Chart(ydp_dates_to_mark).mark_text(
    color='#e45756',
    angle=0
).encode(
    x=alt.X('text_date'+':T',axis=alt.Axis(labels=True,title='')),
    y=alt.Y('height',axis=alt.Axis(labels=True,title='Amount')),
    text='text'
)

yluna_chart = (c1 + c2 + c3).properties(height=350).configure_view(strokeOpacity=0)

all_deltas = pdp_asset_used.append(pdp_asset_unused)
all_deltas = pdp.fill_date_gaps(all_deltas, ['2022-02-11','2022-02-12','2022-02-13'])
c1 = cp.get_yluna_time_area_chart(all_deltas, 
               alt.Scale(scheme='set2'),
               min_date = all_deltas.Time.min(),
               max_date = all_deltas.Time.max(),
               top_padding = 1500000
        )

c2 = alt.Chart(pdp_dates_to_mark).mark_rule(color='#e45756').encode(
    x=alt.X('date'+':T')
)

c3 = alt.Chart(pdp_dates_to_mark).mark_text(
    color='#e45756',
    angle=0
).encode(
    x=alt.X('text_date'+':T',axis=alt.Axis(labels=True,title='')),
    y=alt.Y('height',axis=alt.Axis(labels=True,title='Amount')),
    text='text'
)

pluna_chart = (c1 + c2 + c3).properties(height=350).configure_view(strokeOpacity=0)
perc_amps_chart = xPrismAmpsChart.chart(perc_amps_n_user)
prism_emitted_chart = pe_cp.prism_emitted_chart(pe_dp.prism_emitted, pe_dp.prism_emitted_so_far, 
                       pe_dp.dates_to_mark, pe_dp.extra_dates_to_mark, '2022-05-25')
col0,col1, col2 = st.columns([0.1,1,2])
with col1:
    st.subheader('PRISM Farm Emission')
    st.markdown("""Lorem ipsum dolor sit amet, consectetur adipiscing elit, 
    sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
    Ut enim ad minim veniam, quis nostrud exercitation ullamco 
    laboris nisi ut aliquip ex ea commodo consequat.""")
    st.markdown("""Duis aute irure dolor in reprehenderit in voluptate velit esse 
    cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, 
    sunt in culpa qui officia deserunt mollit anim id est laborum.""")
with col2:
    st.text("")
    st.text("")
    st.altair_chart(prism_emitted_chart.properties(height=350), use_container_width=True)

st.text("")
st.text("")
st.text("")
st.text("")
st.text("")
st.text("")

col1, col2, col0 = st.columns([2,1,0.1])
with col2:
    st.subheader('yLUNA Usage')
    st.markdown("""Lorem ipsum dolor sit amet, consectetur adipiscing elit, 
    sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
    Ut enim ad minim veniam, quis nostrud exercitation ullamco 
    laboris nisi ut aliquip ex ea commodo consequat.""")
    st.markdown("""Duis aute irure dolor in reprehenderit in voluptate velit esse 
    cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, 
    sunt in culpa qui officia deserunt mollit anim id est laborum.""")
with col1:
    st.altair_chart(yluna_chart, use_container_width=True)

st.text("")
st.text("")
st.text("")
st.text("")
st.text("")
st.text("")

col1, col2, col0 = st.columns([2,1,0.1])
with col2:
    st.subheader('pLUNA Usage')
    st.markdown("""Lorem ipsum dolor sit amet, consectetur adipiscing elit, 
    sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
    Ut enim ad minim veniam, quis nostrud exercitation ullamco 
    laboris nisi ut aliquip ex ea commodo consequat.""")
    st.markdown("""Duis aute irure dolor in reprehenderit in voluptate velit esse 
    cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, 
    sunt in culpa qui officia deserunt mollit anim id est laborum.""")
with col1:
    st.altair_chart(pluna_chart, use_container_width=True)

st.text("")
st.text("")
st.text("")
st.text("")
st.text("")
st.text("")

col0, col1, col2 = st.columns([0.1,1,2])
with col1:
    st.subheader('Refraction')
    st.markdown("""Lorem ipsum dolor sit amet, consectetur adipiscing elit, 
    sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
    Ut enim ad minim veniam, quis nostrud exercitation ullamco 
    laboris nisi ut aliquip ex ea commodo consequat.""")
    st.markdown("""Duis aute irure dolor in reprehenderit in voluptate velit esse 
    cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, 
    sunt in culpa qui officia deserunt mollit anim id est laborum.""")
with col2:
    st.text("")
    st.altair_chart(cp.refraction_asset_time(all_refracts).properties(height=350), use_container_width=True)

st.text("")
st.text("")
st.text("")
st.text("")
st.text("")
st.text("")

col1, col2, col0 = st.columns([2,1,0.1])
with col2:
    st.subheader('xPRISM pldged to AMPs')
    st.markdown("""Lorem ipsum dolor sit amet, consectetur adipiscing elit, 
    sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
    Ut enim ad minim veniam, quis nostrud exercitation ullamco 
    laboris nisi ut aliquip ex ea commodo consequat.""")
    st.markdown("""Duis aute irure dolor in reprehenderit in voluptate velit esse 
    cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, 
    sunt in culpa qui officia deserunt mollit anim id est laborum.""")
with col1:
    st.text("")
    st.altair_chart(perc_amps_chart.properties(height=350), use_container_width=True)

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
