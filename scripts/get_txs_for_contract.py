import requests
import pandas as pd

contract = 'terra1argcazqn3ukpyp0vmldxnf9qymnm6vfjaar94g'
txs = requests.get(f"https://api.extraterrestrial.money/v1/txs/by_account?account={contract}&order=desc&offset=0&limit=100").json()

i = 0
offset = 100
tx_hashes = []
while(True):
    url = f"https://api.extraterrestrial.money/v1/txs/by_account?account={contract}&order=desc&offset={i}&limit={offset}"
    txs = requests.get(url).json()
    orig_size = len(tx_hashes)
    try:
        tx_hashes = [*tx_hashes,*list(map(lambda x: (x['txhash'],x['timestamp']),txs['txs']))]
        if(len(tx_hashes)%500==0):
            print(len(tx_hashes))
        i+=offset
    except Exception as e:
        print(e)
        print(url)
        print(txs)
    if(orig_size==len(tx_hashes)):
        break

txs = pd.DataFrame(tx_hashes, columns=['hash','timestamp'])
txs.to_csv(f'../data/txs/{contract}.csv')
txs