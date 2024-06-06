#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import pandas as pd
import numpy as np

from bot_db import DBMySQL
from remote import RemoteServer
from uniswap import amount_out_v2

PARAMETERS_FILE = "~/git/scanner_research/llm_contract_scanner/parameters.json"
TEST_AMOUNT_ETH = 0.05 #ETH
TEST_AMOUNT_ETH18 = TEST_AMOUNT_ETH * 1e18 #ETH

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None


def get_pair(pair_id):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_pair(pair_id)

def get_pair_history(pair_id):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_event_history(pair_id)


#!!! the pair
pair_id = 270008



pair_data = get_pair(pair_id)
pair_history = get_pair_history(pair_id)
pair_history = {p["event_id"]: p for p in pair_history}

print(pair_data[0])
dtypes = {'pair_id': "int64", 'transactionHash': "object", 'block_number': "int", 'timeStamp': "datetime64", 'operation': "object",
       'sender': "object", 'amount0': "object", 'amount1': "object", 'amount0In': "object", 'amount1In': "object", 'amount0Out': "object",
       'amount1Out': "object"}
df_pair_history = pd.DataFrame.from_dict(pair_history, orient="index", dtype="object")

df_pair_history["reserve0"] = (
    df_pair_history["amount0"].cumsum() +
    df_pair_history["amount0In"].cumsum() -
    df_pair_history["amount0Out"].cumsum())
df_pair_history["reserve1"] = (
    df_pair_history["amount1"].cumsum() +
    df_pair_history["amount1In"].cumsum() -
    df_pair_history["amount1Out"].cumsum())

is_token_0 = pair_data[0]["token"] == pair_data[0]["token0"]
if pair_data[0]["decimals"] <= 18:
    multiplier = 10 ** (18 - pair_data[0]["decimals"])
    multiplier1 = 1
else:
    multiplier = 1
    multiplier1 = 10 ** (pair_data[0]["decimals"] - 18)


if is_token_0:
    df_pair_history["price_simple"] = (
        np.double(df_pair_history["reserve0"] * multiplier) /
        np.double(df_pair_history["reserve1"]) / multiplier1)
    df_pair_history.loc[df_pair_history["operation"]=="swap V2", "price_trade"] = (
        np.double((df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount0In"] +
        df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount0Out"]) * multiplier) /
        np.double(df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount1In"] +
        df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount1Out"]) / multiplier1)
    df_pair_history["price_amount_buy"] = amount_out_v2(
        TEST_AMOUNT_ETH18,
        np.double(df_pair_history["reserve1"]) * multiplier1,
        np.double(df_pair_history["reserve0"]) * multiplier) / TEST_AMOUNT_ETH18
    test_amount_token = TEST_AMOUNT_ETH18 * np.double(df_pair_history["reserve0"]) * multiplier / np.double(df_pair_history["reserve1"] / multiplier1)
    df_pair_history["price_amount_sell"] = test_amount_token / amount_out_v2(
        test_amount_token,
        np.double(df_pair_history["reserve0"]) * multiplier,
        np.double(df_pair_history["reserve1"]) * multiplier1)
else:
    df_pair_history["price_simple"] = (
        np.double(df_pair_history["reserve1"] * multiplier) / 
        np.double(df_pair_history["reserve0"]) / multiplier1)
    df_pair_history.loc[df_pair_history["operation"]=="swap V2", "price_trade"] = (
        np.double((df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount1In"] +
        df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount1Out"]) * multiplier) / 
        np.double(df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount0In"] +
        df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount0Out"]) /  multiplier1)
    df_pair_history["price_amount_buy"] = amount_out_v2(
        TEST_AMOUNT_ETH18,
        np.double(df_pair_history["reserve0"]) * multiplier1,
        np.double(df_pair_history["reserve1"]) * multiplier) / TEST_AMOUNT_ETH18
    test_amount_token = TEST_AMOUNT_ETH18 * np.double(df_pair_history["reserve1"]) * multiplier / np.double(df_pair_history["reserve0"] / multiplier1)
    df_pair_history["price_amount_sell"] = test_amount_token / amount_out_v2(
        test_amount_token,
        np.double(df_pair_history["reserve1"]) * multiplier,
        np.double(df_pair_history["reserve0"]) * multiplier1)
