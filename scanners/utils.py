#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import threading

import eth_abi
from web3 import Web3
from token_abi import token_abi

HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'
MAX_RETRY = 10
RED = "\033[1;31m"
GREEN = "\033[0;32m"
BLUE = "\033[0;34m"
RESET_COLOR = "\033[0;0m"


def hex_to_gwei(hex_value):
    try:
        gwei_value = round(int(hex_value, 0)/1000000000, 9)
    except:
        gwei_value = round(int(hex_value)/1000000000, 9)
    return gwei_value

def hex_to_eth(hex_value):
    try:
        eth_value = round(int(hex_value, 0)/1000000000000000000, 18)
    except:
        eth_value = round(int(hex_value)/1000000000000000000, 18)
    return eth_value

def gwei_to_wei(gwei):
    return int(gwei * 1000000000)

def eth_to_wei(eth):
    return int(eth * 1000000000 * 1000000000)

def _get_abi(context, address):
    try:
        res = requests.get(ETHERSCAN_GETABI.format(address, context["etherscan_key"]), headers=HEADERS)
        d = res.json()
        abi = d["result"]
        return abi
    except:
        return None

def _get_contract(w3, abi, address):
    return w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)

def get_contract_sync(context, address):
    for i in range(MAX_RETRY):
        if address in context["abi_storage"]:
            break
        abi = _get_abi(context, address)
        if not abi and i < MAX_RETRY-1:
            time.sleep(5)
        else:
            context["abi_storage"][address] = abi
    else:
        return None
    try:
        contract = _get_contract(context["w3"], context["abi_storage"][address], address)
    except:
        contract = None
    return contract

def get_contract_standard_token(w3, address):
    return w3.eth.contract(address=Web3.to_checksum_address(address), abi=token_abi)

class AtomicInteger():
    def __init__(self, value=0):
        self._value = int(value)
        self._lock = threading.Lock()
        
    def inc(self, d=1):
        with self._lock:
            self._value += int(d)
            return self._value

    def dec(self, d=1):
        return self.inc(-d)    

    def update(self, d):
        with self._lock:
            if self._value < d:
                self._value = d
            return self._value

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        with self._lock:
            self._value = int(v)
            return self._value


