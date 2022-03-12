import requests
import pandas as pd

contract = 'terra1persuahr6f8fm6nyup0xjc7aveaur89nwgs5vs'
txs = requests.get(f"https://api.extraterrestrial.money/v1/txs/by_account?account={contract}&order=desc&offset=0&limit=100").json()

limit = 100
offset = 0
tx_hashes = []
i = 0
while(True):
    url = f"https://api.extraterrestrial.money/v1/txs/by_account?account={contract}&order=desc&offset={offset}&limit={limit}"
    txs = requests.get(url).json()
    orig_size = len(tx_hashes)
    try:
        i += len(txs['txs'])
        tx_hashes = [*tx_hashes,*list(map(lambda x: (x['txhash'],x['timestamp']),filter(lambda x: 'code' not in x,txs['txs'])))]
        if(i%500==0):
            print(len(tx_hashes))
        if(i%2000==0):
            txs = pd.DataFrame(tx_hashes, columns=['hash','timestamp'])
            txs.to_csv(f'./data/txs/{contract}.csv')
        offset+=limit
    except Exception as e:
        print(e)
        print(url)
        print(txs)
    if(orig_size==len(tx_hashes)):
        break

txs = pd.DataFrame(tx_hashes, columns=['hash','timestamp'])
txs.to_csv(f'./data/txs/{contract}.csv')
txs


