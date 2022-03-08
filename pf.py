import streamlit as st
import pandas as pd
import altair as alt
from charts import ChartProvider
import requests
from PIL import Image
from libraries.prism_analytics import DataProvider, ChartProvider

st.set_page_config(page_title="Prism Forge - Analytics",\
        page_icon=Image.open(requests.get('https://raw.githubusercontent.com/IncioMan/prism_forge/master/images/xPRISM.png',stream=True).raw),\
        layout='wide')

###

@st.cache(ttl=3000, show_spinner=False, allow_output_mutation=True)
def claim(claim_hash, cols_claim):
    try:
        df_claim = pd.read_json(
            f"https://api.flipsidecrypto.com/api/v2/queries/{claim_hash}/data/latest",
            convert_dates=["BLOCK_TIMESTAMP"],
        )
    except:
        return pd.DataFrame(columns = cols_claim[claim_hash])
    if(len(df_claim.columns)==0):
        return pd.DataFrame(columns = cols_claim[claim_hash])
    return df_claim

@st.cache(ttl=3000, show_spinner=False, allow_output_mutation=True)
def get_url(url):
    return pd.read_csv(url, index_col=0)
    
cp = ChartProvider()
dp = DataProvider(claim,get_url,path_to_data='./data')
dp.load_from_csv()
dp.polish()

###
###
all_deltas = dp.daily_delta_rf.append(dp.daily_delta_stk).append(dp.daily_delta_lp).append(dp.daily_delta_stk_farm)
chart = cp.get_line_chart(all_deltas, 
               domain = ['yLuna staked','yLuna circulating','yLuna LP','yLuna Farm staked'],
               range_ = ['#f8936d','lightblue','green', 'red'],
               min_date = all_deltas.Time.min(),
               max_date = all_deltas.Time.max(),
               top_padding = 10000
        )
st.subheader('Distribution across deposit and withdrawals percentage buckets')
st.markdown("""This graph shows the number of users which had deposited a specific amount and withdrawn a specific percentage""")
st.altair_chart(chart, use_container_width=True)

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
            padding-left: 15rem;
            padding-right: 15rem;
        }
    }
    .block-container
    {
        padding-bottom: 1rem;
        padding-top: 5rem;
    }
</style>
""", unsafe_allow_html=True)
hide_streamlit_style = """
                        <style>
                        
                        footer {visibility: hidden;}
                        </style>
                        """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)
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
