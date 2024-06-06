#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# https://docs.uniswap.org/contracts/v2/reference/smart-contracts/router-02
# https://docs.uniswap.org/contracts/universal-router/technical-reference

import websocket
import requests
from web3 import Web3
import eth_abi
import json
import time
from datetime import datetime
import pandas as pd
import numpy as np
from threading import Thread
from queue import Queue

import commands_sol

KEY_FILE = 'alchemy.sec'
ETHERSCAN_KEY_FILE = 'etherscan.sec'
RED = "\033[1;31m"
RESET_COLOR = "\033[0;0m"

HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'
PAYLOAD = {"jsonrpc":"2.0",
                        "id": 2, 
                        "method": "eth_subscribe", 
                        "params": ["alchemy_pendingTransactions", 
                                   {"toAddress": []}]}

PAYLOAD2 = {"jsonrpc":"2.0",
                        "id": 3, 
                        "method": "eth_subscribe", 
                        "params": ["alchemy_minedTransactions", 
                                   {"addresses": []}]}

def hex_to_gwei(hex_value):
    return round(int(hex_value, 0)/1000000000, 9)

def hex_to_eth(hex_value):
    return round(int(hex_value, 0)/1000000000000000000, 18)

def extract_path_from_V3(str_path):
    path = []
    i = 0
    while i < len(str_path):
        path.append("0x" + str_path[i:i+20].hex())
        i = i + 23
    return path

def uniswap_transaction(tx):
    fn_name = tx["decoded_input"][0].abi["name"]
    analytics = {"function": fn_name,
                "gas_price": hex_to_gwei(tx["gasPrice"]),
                "value": hex_to_eth(tx["value"])}
    if fn_name == 'execute':
        command_codes = tx["decoded_input"][1]["commands"]
        commands = []
        inputs = []
        first_none_zero_input = None
        for i, command_code in enumerate(command_codes):
            command = commands_sol.uniswap_universal_router_code_to_command(command_code)
            commands.append(command)
            abi = commands_sol.uniswap_universal_router_get_abi(command)
            if abi:
                none_zero_input = eth_abi.abi.decode(abi, tx["decoded_input"][1]["inputs"][i])
                inputs.append(none_zero_input)
                if not first_none_zero_input:
                    first_none_zero_input = none_zero_input
                    analytics["function"] = command
            else:
                inputs.append(None)
               
        analytics["commands"] = commands
        if first_none_zero_input:
            analytics["amount_in"] = first_none_zero_input[1]
            analytics["amount_out_min"] = first_none_zero_input[2]
            if analytics["function"][:2] == "V3":
                analytics["tokens"] = {a.lower(): "" for a in extract_path_from_V3(first_none_zero_input[3])}
            else:
                analytics["tokens"] = {a.lower(): "" for a in first_none_zero_input[3]}
        return analytics
    if "amountIn" in tx["decoded_input"][1]:
        analytics["amount_in"] = tx["decoded_input"][1]["amountIn"]
    if "amountOutMin" in tx["decoded_input"][1]:
        analytics["amount_out_min"] = tx["decoded_input"][1]["amountOutMin"]
    
    analytics["tokens"] = {}
    if "path" in tx["decoded_input"][1]:
        for a in tx["decoded_input"][1]["path"]:
            analytics["tokens"][a.lower()] = ""
    return analytics


target_adresses = {"Uniswap_V2_Router_2": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".lower(),
            "Uniswap_Universal_Router": "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b".lower(),
            "UniswapV3SwapRouter02": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower(),
            "UniversalRouter": "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower()}

parse_functions = {"Uniswap_V2_Router_2": uniswap_transaction,
            "Uniswap_Universal_Router": uniswap_transaction,
            "UniswapV3SwapRouter02": uniswap_transaction,
            "UniversalRouter": uniswap_transaction}

pending_transactions = {}
mined_transactions = {}
all_messages = []
abi_storage = {}
contract_storage = {}

try:
    with open(KEY_FILE, 'r') as f:
        k1 = f.readline()
        alchemy_url = k1.strip('\n')
        k2 = f.readline()
        alchemy_wss = k2.strip('\n')
except:
    alchemy_url = ""
    alchemy_wss = ""

try:
    with open(ETHERSCAN_KEY_FILE, 'r') as f:
        k1 = f.readline()
        etherscan_key = k1.strip('\n')
except:
    etherscan_key = ""


class WebSocketListener:
    def __init__(self, alchemy_wss,
                 alchemy_url,
                 target_adresses,
                 parse_functions,
                 pending_transactions,
                 mined_transactions,
                 all_messages,
                 abi_storage,
                 contract_storage):
        self.uri = alchemy_wss
        self.w3 = Web3(Web3.HTTPProvider(alchemy_url))
        self.ws_pending = websocket.WebSocketApp(
            alchemy_wss,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open)
        self.ws_mined = websocket.WebSocketApp(
            alchemy_wss,
            on_message=self.on_message2,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open2)
        
        self.target_adresses = target_adresses
        self.parse_functions = parse_functions
        self.all_messages = all_messages
        self.pending_transactions = pending_transactions
        self.mined_transactions = mined_transactions
        self.abi_storage = abi_storage
        self.contract_storage = contract_storage
        self.payload = PAYLOAD
        self.payload2 = PAYLOAD2
        self.payload["params"][1]["toAddress"] = list(target_adresses.values())
        self.payload2["params"][1]["addresses"] = [{"to": a} for a in target_adresses.values()]
        
        self._thread_websocket = Thread(target=self.ws_pending.run_forever)
        self._thread_websocket2 = Thread(target=self.ws_mined.run_forever)
        self._thread_etherscan = Thread(target=self.abi_queue)
        self._thread_contract = Thread(target=self.contract_queue)
        self.run_threads = True
        self.queue_etherscan = Queue()
        self.queue_contract = Queue()
        self.unprocessed_pool = set()
        self.unknoun_tokens_pools = {}
        self.requested_abi = set()
        self.catched = 0

    def on_message(self, ws, message_string):
        self.all_messages.append(message_string)
        tx_json = json.loads(message_string)
        if not 'params' in tx_json:
            return
        tx = tx_json['params']['result']
        tx["received"] = datetime.utcnow().timestamp()
        self.pending_transactions[tx["hash"]] = tx
        tx["scanner_processed"] = self.process_transaction(tx)
        if not tx["scanner_processed"]:
            self.unprocessed_pool.add(tx["hash"])

    def on_message2(self, ws, message_string):
        tx_json = json.loads(message_string)
        if not 'params' in tx_json:
            return
        tx = tx_json['params']['result']["transaction"]
        tx["received"] = datetime.utcnow().timestamp()
        self.mined_transactions[tx["hash"]] = tx
        if not tx["hash"] in self.pending_transactions:
            pass
            # print(len(self.pending_transactions), len(self.mined_transactions), self.catched/len(self.mined_transactions),
                  # int(tx["blockNumber"], 0), tx["hash"], flush=True)
        else:
            self.catched += 1

    def on_error(self, ws, error):
        print(error)
    
    def on_close(self, ws, *args):
        if self.run_threads:
            time.sleep(1)
            print("restart after 1 sec", flush=True)
            ws.run_forever()

    def on_open(self, ws):
        payload_json = json.dumps(self.payload)
        self.ws_pending.send(payload_json)

    def on_open2(self, ws):
        payload_json = json.dumps(self.payload2)
        self.ws_mined.send(payload_json)

    def abi_queue(self):
        def get_abi(address):
            try:
                res = requests.get(ETHERSCAN_GETABI.format(address, etherscan_key), headers=HEADERS)
        #!!! no error processing
                d = res.json()
                abi = d["result"]
                return abi
            except:
                return None

        while self.run_threads:
            if self.queue_etherscan.qsize() > 0:
                address = self.queue_etherscan.get()
                self.abi_storage[address] = get_abi(address)
                self.requested_abi.remove(address)
                time.sleep(0.3)
            time.sleep(0.1)

    def contract_queue(self):
        def get_contract(w3, address, abi):
            return w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)
        #!!! no error processing

        while self.run_threads:
            address_list = []
            while self.queue_contract.qsize() > 0:
                address_list.append(self.queue_contract.get())
            
            for address in address_list:
                if address in abi_storage:
                    retry_address = False
                    try:
                        self.contract_storage[address] = get_contract(self.w3, address, abi_storage[address])
                    except:
                        self.queue_contract.put(address)
                        continue
                    tx_processed_list = []
                    for tx_hash in self.unprocessed_pool:
                        if not tx_hash in self.pending_transactions:
                            retry_address = True
                            print("no tx", tx_hash, flush=True)
                        elif address == self.pending_transactions[tx_hash]["to"]:
                            self._process_transaction(self.pending_transactions[tx_hash])
                            self.pending_transactions[tx_hash]["scanner_processed"] = True
                            tx_processed_list.append(tx_hash)
                    for tx_hash in tx_processed_list:
                        self.unprocessed_pool.remove(tx_hash)

                    if address in self.unknoun_tokens_pools:
                        found_token_tx = []
                        for tx_hash in self.unknoun_tokens_pools[address]:
                            if not tx_hash in self.pending_transactions:
                                retry_address = True
                                print("no tx", tx_hash, flush=True)
                            else:
                                self._process_transaction_fill_tokens(self.pending_transactions[tx_hash])
                                found_token_tx.append(tx_hash)
                        for tx_hash in found_token_tx:
                            self.unknoun_tokens_pools[address].remove(tx_hash)
                    if retry_address:
                        self.queue_contract.put(address)
                else:
                    self.queue_contract.put(address)
                    if not address in self.requested_abi:
                        self.queue_etherscan.put(address)
                        self.requested_abi.add(address)
            time.sleep(0.1)

    def try_fill_symbol(self, t):
        try:
            return self.contract_storage[t].functions.symbol().call()
        except:
            try:
                print("no symbol", t, self.contract_storage[t].functions.name().call(), flush=True)
            except:
                try:
                    symbol_dict = {'inputs': [], 'name': 'symbol', 'outputs': [{'internalType': 'string', 'name': '', 'type': 'string'}], 'stateMutability': 'view', 'type': 'function'}
                    lame_abi = json.loads(self.abi_storage[t])
                    lame_abi.append(symbol_dict)
                    abi_str = json.dumps(lame_abi)
                    fixed_contract = self.w3.eth.contract(address=Web3.to_checksum_address(t), abi=abi_str)
                    symbol = fixed_contract.functions.symbol().call()
                    self.abi_storage[t] = abi_str
                    self.contract_storage[t] = fixed_contract
                    return symbol
                except:
                    print("no symbol", t, "no name", flush=True)
            return("noname")

    def _process_transaction_fill_tokens(self, tx):
        not_found = 0
        if not "analytics" in tx or "tokens" in tx["analytics"]:
            return 0
        for t in tx["analytics"]["tokens"]:
            if t in self.contract_storage:
                tx["analytics"]["tokens"][t] = self.try_fill_symbol(t)
            if not tx["analytics"]["tokens"][t]:
                not_found += 1
        return not_found

    def _process_transaction(self, tx):
        address = tx["to"].lower()
        tx["decoded_input"] = self.contract_storage[address].decode_function_input(tx["input"])

        for ta in target_adresses:
            if tx['to'].lower() == target_adresses[ta]:
                analytics = self.parse_functions[ta](tx)
                analytics["pool"] = ta
                
                if "tokens" in analytics:
                    for t in analytics["tokens"]:
                        if t in self.contract_storage:
                            analytics["tokens"][t] = self.try_fill_symbol(t)
                        else:
                            self.queue_contract.put(t)
                            if not t in self.unknoun_tokens_pools:
                                self.unknoun_tokens_pools[t] = set()
                            self.unknoun_tokens_pools[t].add(tx["hash"])
                tx["analytics"] = analytics

                # print(tx["hash"], analytics)
        # print(len(self.pending_transactions))
        
    def process_transaction(self, tx):
        address = tx["to"].lower()
        if address in contract_storage:
            self._process_transaction(tx)
            return True
        else:
            self.queue_contract.put(address)
            return False

    def start(self):
        self._thread_websocket.start()
        self._thread_websocket2.start()
        self._thread_etherscan.start()
        self._thread_contract.start()
        
        for a in target_adresses:
            if not TARGET_ADRESSES[a] in contract_storage:
                self.queue_contract.put(target_adresses[a])
        
    def stop(self):
        self.run_threads = False
        self.ws_pending.keep_running = False
        self.ws_pending.close()
        self.ws_mined.keep_running = False
        self.ws_mined.close()


def main():

    wsl = WebSocketListener(alchemy_wss,
                           alchemy_url,
                           target_adresses,
                           parse_functions,
                           pending_transactions,
                           mined_transactions,
                           all_messages,
                           abi_storage,
                           contract_storage)
    
    wsl.start()    ### to run :

### to stop:
    # wsl.stop()


    # for tx in pending_transactions:
    #     if pending_transactions[tx]["scanner_processed"] and pending_transactions[tx]["analytics"]["function"] != "execute":
    #         print("transaction", tx, pending_transactions[tx]["analytics"])

def garbage_zone(wsl):
    
    wsl.stop()

    # abi_storage[web_socket_listener.queue_contract.get()]

    wsl._thread_websocket2
    wsl._thread_websocket
    wsl._thread_etherscan
    wsl._thread_contract

    
    #!!! 0x32ff4b1156bc00ee93ccef79fc30b5075fe2a7829136928a2d17fbdaf09ae7d4
    #  'commands': ['V3_SWAP_EXACT_IN', 'V2_SWAP_EXACT_IN']

    contract_storage["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"].functions.name().call()
    
   
    df_p = pd.DataFrame.from_records([{"hash": t,
                                       "to": pending_transactions[t]["to"],
                                       "started": pending_transactions[t]["received"]} for t in pending_transactions])
    df_p.set_index(["hash"], inplace=True, drop=True)
    df_p["pending"] = 1

    df_m = pd.DataFrame.from_records([{"hash": t,
                                       "to": mined_transactions[t]["to"],
                                       "block": int(mined_transactions[t]["blockNumber"], 0),
                                       "finalized": mined_transactions[t]["received"]} for t in mined_transactions])
    df_m.set_index(["hash"], inplace=True, drop=True)
    df_m["started"] = np.nan
    df_m["pending"] = 0
    df_m.update(df_p)
    df_m["completed_in"] = df_m["finalized"] - df_m["started"]
    df_m.loc[df_m["completed_in"] < 3, "pending"] = 0
    
    qqq = df_m.loc[df_m["completed_in"] < 0]
    qqq.sort_values(["completed_in"]).index
    
    eee = pending_transactions["0x209c12a8c1594eccb9de278ec8ce81bfffe3fd117459bf895db0542b4c08c69d"]
    eee1 = mined_transactions["0x209c12a8c1594eccb9de278ec8ce81bfffe3fd117459bf895db0542b4c08c69d"]
    
    df_m.loc[df_m["completed_in"] > 0, "completed_in"].hist(bins=100)

    df_pivot = df_m.groupby(["block"]).agg({"pending": ["mean", "count"], "completed_in": ["min", "max", "mean"]})
    
    R_DIR = "/media/Data/csv/"
    df_pivot.to_csv(R_DIR + "tx_pended_mined.csv")
    
