import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import time
api_key = ""

root_url = "https://api.arkhamintelligence.com"
headers = {
    "API-Key": api_key
}
funds = {
    'Grayscale': "grayscale",
    'BlackRock': "blackrock",
    'Fidelity': "fidelity",
    'Bitwise': "bitwise",
    'VanEck': "vaneck",
    'MicroStrategy': "microstrategy",
    'ARK Invest': "ark-invest",
}
cexes ={
    'Binance': "binance",
    'Coinbase': "coinbase",
    'OKX': "okx",
    'Bitfinex': "bitfinex",
    'Bybit': "bybit",
    'Kraken': "kraken",
}
tokens = {
    'BTC': "bitcoin",
    'ETH': "ethereum",
    'STX': "blockstack",
    'JUP': "jupiter-exchange-solana",
    'SOL': "solana",
    'TIA': "celestia",
    "INJ": "injective-protocol",
    "SEI": "sei-network",
    "DYM": "dymension",
    "ARKM" : "arkham",
    'LINK': "chainlink",
    "PYTH": "pyth-network",
    "ORDI": "ordinals",
    "ARB": "arbitrum"
}
cexAndFundsArr = list(cexes.values()) + list(funds.values())
onlyCexArr = list(cexes.values())
etherscanRoot = f'https://etherscan.io/tx/'
bitcoinRoot = f'https://blockstream.info/tx/'
arkhamEntityRoot = f'https://platform.arkhamintelligence.com/explorer/entity/'
arkhamAddressRoot = f'https://platform.arkhamintelligence.com/explorer/address/'
arkhamTxRoot = f'https://platform.arkhamintelligence.com/explorer/tx/'
chatId = '-'
root_url_telegram = 'https://api.telegram.org/bot:'
send_path = f"{root_url_telegram}/sendMessage?chat_id={chatId}&text="
textBeforeMsg_1h = "Last 1 hour netflow of tokens of exchanges: \n"
textBeforeMsg_4h = "Last 4 hours netflow of tokens of exchanges: \n"
textBeforeMsg_12h = "Last 12 hours netflow of tokens of exchanges: \n"
textBeforeMsg_3d = "Last 3 days netflow of tokens of exchanges: \n"


class arkham_transfer: # path = "/transfer"

        def __init__(self, base=[], chains=[], flow="all", From=[], to=[], tokens=[], timeGte=0,
                    timeLte=0, timeLast="1m", valueGte=0, valueLte=0, usdGte=0, usdLte=0, sortKey="value", sortDir="desc", limit=20, offset=100):
            self.params = {
                "base" : base, # ftx, binance, grayscale...
                "chains" : chains, # ethereum, bitcoin...
                "flow" : flow, # in, out, default = all
                "from" : From,
                "to" : to, # just like from
                "tokens" : tokens, # tokens address
                "timeGte" : timeGte, #篩選出區塊_時間戳大於或等於特定時間的交易。
                "timeLte" : timeLte, #篩選出區塊_時間戳<於或等於特定時間的交易。
                "timeLast" : timeLast, # last 1d, 1h, 1m, 1s
                "valueGte" : valueGte, # 篩選出超過特定代幣單位價值的轉帳資訊
                "valueLte" : valueLte, # 篩選出<過特定代幣單位價值的轉帳資訊
                "usdGte" : usdGte, # just like valueGte, but in usd base format
                "usdLte" : usdLte,
                "sortKey" : sortKey, # "time", "value", "usd"
                "sortDir" : sortDir, # "asc", "desc"
                "limit" : limit, # 返回的轉帳結果數量，預設至20筆。最多10,000筆。
                "offset" : offset,
            }
            self.url = "/transfers"

        def set_params(self, params):
            self.params = params
        def get_params(self):
            return self.params
        def request_tx(self):
            data = requests.get(url=f'{root_url}{self.url}', headers=headers, params=self.params)
            if data.status_code == 200:
                return data.json()
            else:
                print("Error! "+str(data.status_code))
                return None
def convertTimeStampToUTCTime(time):
    utc_time = datetime.utcfromtimestamp(float(time))
    utc_time_plus_8 = utc_time + timedelta(hours=16)
    date_str = utc_time_plus_8.strftime('%Y-%m-%d %H:%M:%S')
    return date_str
def convertBlockTimeToUTCTime(blockTimestamp):
    datetimeobj = datetime.strptime(blockTimestamp, '%Y-%m-%dT%H:%M:%SZ')
    timestamp = datetimeobj.timestamp()
    return convertTimeStampToUTCTime(timestamp)
def get_cur_timestamp():
        # Get the current time in UTC+8
    current_time = datetime.now()
    # Calculate the start time for the last 60 minutes
    start_time = current_time

    # Convert the start time to a timestamp in milliseconds
    active_timeStamp = int(start_time.timestamp())*1000

    utc_time = datetime.utcfromtimestamp(active_timeStamp/1000) + timedelta(hours=8)
    date_str = utc_time.strftime('%Y-%m-%d %H:%M:%S')
    return date_str
obj = arkham_transfer()

def findTheMostValueAddress(addressList):
    resEntity, resAddress, resLabel, resChain, bestValue  = None, None, None, None, None
    try:
        if addressList == None: return None, None, None, None, None
        for address in addressList:
            each = address['address']
            curValue = address['value']
            if bestValue == None:
                bestValue = curValue
                if 'arkhamLabel' in each: resLabel = each['arkhamLabel']['name']
                if 'chain' in each:
                    resChain = each['chain']
                if 'arkhamEntity' in each:
                    resEntity = each['arkhamEntity']['name']
                if 'address' in each:
                    resAddress = each['address']
            if curValue > bestValue:
                if 'arkhamLabel' in each: resLabel = each['arkhamLabel']['name']
                if 'chain' in each:
                    resChain = each['chain']
                if 'arkhamEntity' in each:
                    resEntity = each['arkhamEntity']['name']
                if 'address' in each:
                    resAddress = each['address']
                bestValue = curValue

        return resChain, resEntity, resLabel, resAddress, bestValue
    except Exception as e:
        print(e)
        return resChain, resEntity, resLabel, resAddress, bestValue
def alert(tx):
    msg = None
    try:
        # init all the variable we need
        chain,txhash,block,time,From,to,value = None,None,None,None,None,None,None
        fromArkhamLabeledName, USDValue, toArkhamLabeledName = None, None, None
        fromAddress, toAddress, toEntity, fromEntity, toValue = None, None, None, None, None
        # fromAddresses or fromAddress
        # toAddresses or toAddress
        # to find the token name, the network, the from's entity, and the to's entity
        if 'transactionHash' in tx:
            txhash = tx['transactionHash']
        elif 'txid' in tx:
            txhash = tx['txid']
        if 'blockTimestamp' in tx:
            time =str(convertBlockTimeToUTCTime(tx['blockTimestamp'])) + " UTC+8"
        if 'blockNumber' in tx:
            block = str(tx['blockNumber'])
        elif 'blockHeight' in tx:
            block = str(tx['blockHeight'])
        # from entity
        if 'fromAddress' in tx:
            From = tx['fromAddress']
            if 'arkhamLabel' in From: fromArkhamLabeledName = From['arkhamLabel']['name']
            chain = From['chain']
            if 'arkhamEntity' in From: fromEntity = From['arkhamEntity']['name']
            if 'address' in From: fromAddress = From['address']
        else: # fromAddresses
            addressList = tx['fromAddresses']
            for address in addressList:
                each = address['address']
                if 'arkhamLabel' in each: fromArkhamLabeledName = each['arkhamLabel']['name']
                if 'chain' in each:
                    chain = each['chain']
                if 'arkhamEntity' in each:
                    fromEntity = each['arkhamEntity']['name']
                if 'address' in each:
                    fromAddress = each['address']
                if chain != None and fromEntity != None: break

        # to entity
        if 'toAddress' in tx:
            to = tx['toAddress']
            if 'arkhamLabel' in to: toArkhamLabeledName = to['arkhamLabel']['name']
            chain = to['chain']
            if 'arkhamEntity' in to: toEntity = to['arkhamEntity']['name']
            if 'address' in to: toAddress = to['address']
        else: # toAddresses
            addressList = tx['toAddresses']
            chain, toEntity, toArkhamLabeledName, toAddress, toValue = findTheMostValueAddress(addressList=addressList)

        if toValue == None: toValue = 0
        if 'fromValue' in tx:
            # alpha value
            if 'toValue' in tx:
                toValue = float(tx['toValue'])
            value = tx['fromValue']
            if float(toValue) > float(value):
                value = float(toValue)
        else:
            if 'toValue' in tx:
                toValue = float(tx['toValue'])
            value = tx["unitValue"]
            if float(toValue) > float(value):
                value = float(toValue)
        USDValue = f"(${tx['historicalUSD']} USD)"
        # convert them to readable text
        if chain == 'ethereum':
            arkhamHash = arkhamTxRoot+txhash
            # txhash = etherscanRoot+txhash
            valueFormat = 'ETH'
        elif chain == 'bitcoin':
            arkhamHash = arkhamTxRoot+txhash
            # txhash = bitcoinRoot+txhash
            valueFormat = 'BTC'
        else:
            if chain == 'None': chain = 'unknown, will be updated soon'
            arkhamHash = arkhamTxRoot+txhash
            valueFormat = tx['tokenSymbol']
            print("error: "+txhash)
        if fromArkhamLabeledName == None: fromArkhamLabeledName = '(unlabeled)'
        if toArkhamLabeledName == None: toArkhamLabeledName = '(unlabeled)'
        text1 = f"網路: {chain}"
        text2 = f"交易哈希: {txhash[:5]}..."
        text3 = "狀態: 成功"
        text4 = f"區塊: {block}"
        text5 = f"時間: {time}"
        text6 = f"From: {fromEntity}: {fromArkhamLabeledName} ({fromAddress[:5]})"
        text7 = f"To: {toEntity}: {toArkhamLabeledName} ({toAddress[:5]})"
        text8 = f"價值: {value} {valueFormat} {USDValue}"
        text9 = f"Check it on Arkham! => {arkhamHash}"
        msg = "\n".join([text9, text1, text2, text3, text4, text5, text6, text7, text8])
        requests.get(send_path+msg)
    except Exception as e:
        print("發生異常：", str(e))
        print("txHash: ", txhash)

# someone transfer token from cex to their hot wallet
from_cex_params = {
    "base": onlyCexArr,
    "flow" : "all",
    "from": onlyCexArr,
    "timeLast": "30m",
    "tokens": list(tokens.values()),
    "sortKey": "time",
    "usdGte" : 1, # 篩選出超過特定代幣單位價值的轉帳資訊
    "sortDir": "desc",
    "limit": 10000,
}
# someone transfer token from their hot wallet to cex
to_cex_params = {
    "base": onlyCexArr,
    "flow" : "all",
    "to": onlyCexArr,
    "timeLast": "30m",
    "tokens": list(tokens.values()),
    "sortKey": "time",
    "usdGte" : 1, # 篩選出超過特定代幣單位價值的轉帳資訊
    "sortDir": "desc",
    "limit": 10000,
}
def calculate_net_flow(out_cex, in_cex):
    cexes_tokens_flow = {}
    fromValue, toValue = None, None
    for tx in out_cex:
        try:
            if 'transactionHash' in tx:
                txhash = tx['transactionHash']
            elif 'txid' in tx:
                txhash = tx['txid']
            if 'historicalUSD' in tx: # get the usd value of the token
                USDValue = float(tx['historicalUSD'])
            if 'fromAddress' in tx:
                From = tx['fromAddress']
                chain = From['chain']
            else: # fromAddresses
                addressList = tx['fromAddresses']
                for address in addressList:
                    each = address['address']
                    if 'chain' in each: # once we find the chain, our job is done
                        chain = each['chain']
                        break
            if fromValue == None: fromValue = 0
            if 'fromValue' in tx: # get the token value
                if 'toValue' in tx:
                    fromValue = float(tx['toValue'])
                tokenValue = tx['fromValue']
                if float(fromValue) > float(tokenValue):
                    tokenValue = float(fromValue)
            else:
                if 'toValue' in tx: # get the token value
                    fromValue = float(tx['toValue'])
                tokenValue = tx["unitValue"]
                if float(fromValue) > float(tokenValue):
                    tokenValue = float(fromValue)
            if chain == 'bitcoin': # if the chain is bitcoin, we will use BTC as the token
                tokens = 'BTC'
            elif 'tokenSymbol' in tx:
                tokens = tx['tokenSymbol']
            if USDValue > 50000000: # this is a whales activity, we will alert the user
                alert(tx) # this sends a message to the telegram group
            if tokens not in cexes_tokens_flow:
                cexes_tokens_flow[tokens] = tokenValue
            else:
                cexes_tokens_flow[tokens] += tokenValue # if the token is send from the cex (to any hot wallet), we will add the value
        except Exception as e:
            print("發生異常：", str(e))
            print("txHash: ", txhash)
    for tx in in_cex:
        try:
            if 'transactionHash' in tx:
                txhash = tx['transactionHash']
            elif 'txid' in tx:
                txhash = tx['txid']
            if 'historicalUSD' in tx:
                USDValue = float(tx['historicalUSD'])
            if 'fromAddress' in tx:
                From = tx['fromAddress']
                chain = From['chain']
            else: # fromAddresses
                addressList = tx['fromAddresses']
                for address in addressList:
                    each = address['address']
                    if 'chain' in each:
                        chain = each['chain']
                        break
            if toValue == None: toValue = 0
            if 'fromValue' in tx:
                if 'toValue' in tx:
                    toValue = float(tx['toValue'])
                tokenValue = tx['fromValue']
                if float(toValue) > float(tokenValue):
                    tokenValue = float(toValue)
            else:
                if 'toValue' in tx:
                    toValue = float(tx['toValue'])
                tokenValue = tx["unitValue"]
                if float(toValue) > float(tokenValue):
                    tokenValue = float(toValue)
            if chain == 'bitcoin':
                tokens = 'BTC'
            elif 'tokenSymbol' in tx:
                tokens = tx['tokenSymbol']
            if USDValue > 50000000: # this is a whales activity, we will alert the user
                alert(tx) # this sends a message to the telegram group
            if tokens not in cexes_tokens_flow:
                cexes_tokens_flow[tokens] = -tokenValue
            else:
                cexes_tokens_flow[tokens] -= tokenValue # if the token is sent to the cex, we will minus the value
        except Exception as e:
            print("發生異常：", str(e))
            print("txHash: ", txhash)

    return cexes_tokens_flow

# check cex outflow
def checkNetFlowByTimeframe(timeLast):
    try:
        # check cex outflow, => someone transfer token from cex to their hot wallet
        from_cex_params['timeLast'] = timeLast
        obj.set_params(from_cex_params)
        data = obj.request_tx()
        in_list = data['transfers']


        # check cex inflow
        to_cex_params['timeLast'] = timeLast
        obj.set_params(to_cex_params)
        data = obj.request_tx()
        out_list = data['transfers']


        cexes_tokens_flow = calculate_net_flow(in_list, out_list)
        arr = None
        if timeLast == '1h':
            arr = [textBeforeMsg_1h]
        elif timeLast == '4h':
            arr = [textBeforeMsg_4h]
        elif timeLast == '12h':
            arr = [textBeforeMsg_12h]
        elif timeLast == '3d':
            arr = [textBeforeMsg_3d]

        sorted_cexes = sorted(cexes_tokens_flow.items(), key=lambda item: item[1], reverse=True)
        for cex, token_flow in sorted_cexes:
            token_flow_str = f'{cex}: {token_flow}'
            arr.append(token_flow_str)

        msg = "\n".join(arr)
        requests.get(send_path + msg)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    # get the timeout.json file
    while True:
        timeout = None
        checked = False
        
        # open it, convert it to a dict
        with open('timeout.json', 'r') as f:
            timeout = json.load(f)
            f.close()

        # check the last time the function was called
        # the string of time format will be like this: "2021-10-01 12:00:00"
        # we check it using the
        curTimeFormat = get_cur_timestamp()
        print(curTimeFormat)
        if timeout != None:
            lastTimeFormat_1h = timeout['1h']
            lastTimeFormat_4h = timeout['4h']
            lastTimeFormat_12h = timeout['12h']
            lastTimeFormat_3d = timeout['3d']
            lastTime_1h = datetime.strptime(lastTimeFormat_1h, '%Y-%m-%d %H:%M:%S')
            lastTime_4h = datetime.strptime(lastTimeFormat_4h, '%Y-%m-%d %H:%M:%S')
            lastTime_12h = datetime.strptime(lastTimeFormat_12h, '%Y-%m-%d %H:%M:%S')
            lastTime_3d = datetime.strptime(lastTimeFormat_3d, '%Y-%m-%d %H:%M:%S')
            curTime = datetime.strptime(curTimeFormat, '%Y-%m-%d %H:%M:%S')
            print(curTime)
            print("Checking the timeout 1h, 4h, 12h, 3d...")
            if (curTime - lastTime_1h).seconds > 3600: # if the last time is 1 hour ago, then call the function
                print("Running 1h...")
                timeout['1h'] = curTimeFormat
                checked = True
                checkNetFlowByTimeframe('1h')
            if (curTime - lastTime_4h).seconds > 14400: # if the last time is 4 hours ago, then call the function
                print("Running 4h...")
                timeout['4h'] = curTimeFormat
                checked = True
                checkNetFlowByTimeframe('4h')
            if (curTime - lastTime_12h).seconds > 43200: # if the last time is 12 hours ago, then call the function
                print("Running 12h...")
                timeout['12h'] = curTimeFormat
                checked = True
                checkNetFlowByTimeframe('12h')
            if (curTime - lastTime_3d).days > 3: # if the last time is 3 days ago, then call the function
                print("Running 3d...")
                timeout['3d'] = curTimeFormat
                checked = True
                checkNetFlowByTimeframe('3d')


        # write the dict to the timeout.json file
        if checked == True:
            with open('timeout.json', 'w') as f:
                json.dump(timeout, f)
                f.close()
            time.sleep(3)
        else: 
            time.sleep(1)
        # repeat the process

    # i will put it into the while true loop
    # this program will run 24/7 in my google cloud server

#%%
