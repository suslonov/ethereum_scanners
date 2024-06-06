#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import numpy as np
import decimal
import pickle
import zlib
import json
import MySQLdb

class DBMySQL(object):
    db_host="127.0.0.1"
    db_user="sniper"
    db_passwd="sniper"
    db_name="sniper"

    def __init__(self, port=None):
        self.port = port

    def start(self):
        if self.port:
            self.db_connection = MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_name, port=self.port)
        else:
            self.db_connection = MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_name)
        self.cursor = self.db_connection.cursor()

    def commit(self):
        self.db_connection.commit()

    def stop(self):
        self.db_connection.commit()
        self.db_connection.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        
    def _create_table(self, sql_drop, *sql):
            try:
                self.cursor.execute(sql_drop)
            except:
                pass
            for s in sql:
                self.cursor.execute(s)
        
    def create_tables(self, tables):
        if "t_pairs" in tables:
            s1 = "DROP TABLE t_pairs"
            s2 = "CREATE TABLE t_pairs (pair_id INT NOT NULL PRIMARY KEY, "
            s2 += "pair VARCHAR(256), token0 VARCHAR(256), token1 VARCHAR(256), token VARCHAR(256), token_name VARCHAR(256), token_symbol VARCHAR(256), decimals int, "
            s2 += "swaps int, first_block_number int, price_analytics JSON, chat_gpt JSON, competitors JSON)"
            self._create_table(s1, s2)

        if "t_tokens" in tables:
            s1 = "DROP TABLE t_tokens"
            s2 = "CREATE TABLE t_tokens (token VARCHAR(256) NOT NULL PRIMARY KEY, "
            s2 += "token_name VARCHAR(256), token_symbol VARCHAR(256), decimals int, "
            s2 += "properties JSON, chat_gpt JSON)"
            self._create_table(s1, s2)

        if "t_event_history" in tables:
            s1 = "DROP TABLE t_event_history"
            s2 = "CREATE TABLE t_event_history (event_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, pair_id INT NOT NULL, "
            s2 += "transactionHash VARCHAR(256), block_number int, timeStamp datetime NOT NULL, operation VARCHAR(10), sender VARCHAR(256), "
            s2 += "amount0 DECIMAL(60), amount1 DECIMAL(60), amount0In DECIMAL(60), amount1In DECIMAL(60), amount0Out DECIMAL(60), amount1Out DECIMAL(60))"
            s3 = "ALTER TABLE t_event_history ADD INDEX (pair_id)"
            self._create_table(s1, s2, s3)

        if "t_contract_code" in tables:
            s1 = "DROP TABLE t_contract_code"
            s2 = "CREATE TABLE t_contract_code (token_id VARCHAR(256) NOT NULL PRIMARY KEY, contract_text LONGBLOB, contract_analytics JSON)"
            self._create_table(s1, s2)

    def check_lock_pair(self, pair_id):
        s0 = "select pair_id from t_pairs where pair_id=%s"
        l = self.cursor.execute(s0, (pair_id, ))
        if l:
            return 1
        else:
            s1 = "insert into t_pairs(pair_id) values(%s)"
            self.cursor.execute(s1, (pair_id, ))
            return 0

    def add_pair(self, pair_id, pair, pair_data):
        s0 = "select pair_id from t_pairs where pair_id=%s"
        l = self.cursor.execute(s0, (pair_id, ))
        if l:
            if not "token" in pair_data or pair_data["token"] is None:
                s1 = "update t_pairs set pair=%s, token0=%s, token1=%s  where pair_id = %s"
                self.cursor.execute(s1, (pair, pair_data["token0"], pair_data["token1"], pair_id))
            else:
                s1 = "update t_pairs set pair=%s, token0=%s, token1=%s, token=%s, swaps=%s, first_block_number=%s where pair_id = %s"
                self.cursor.execute(s1, (pair, pair_data["token0"], pair_data["token1"], pair_data["token"],
                                         pair_data["swaps"], pair_data["first_block_number"], pair_id))
                self.add_token(pair_data["token"], pair_data["token_name"], pair_data["token_symbol"], pair_data["decimals"])
        else:
            if not "token" in pair_data or pair_data["token"] is None:
                s1 = "insert into t_pairs(pair_id, pair, token0, token1) values(%s, %s, %s, %s)"
                self.cursor.execute(s1, (pair_id, pair, pair_data["token0"], pair_data["token1"]))
            else:
                s1 = "insert into t_pairs(pair_id, pair, token0, token1, token, swaps, first_block_number) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                self.cursor.execute(s1, (pair_id, pair, pair_data["token0"], pair_data["token1"], pair_data["token"],
                                         pair_data["swaps"], pair_data["first_block_number"]))
                self.add_token(pair_data["token"], pair_data["token_name"], pair_data["token_symbol"], pair_data["decimals"])

    def add_event_history(self, pair_id, data_list):
        if not data_list:
            return
        s0 = "delete from t_event_history where pair_id = " + str(pair_id)
        self.cursor.execute(s0)
        
        s1 = "insert into t_event_history(pair_id, transactionHash, block_number, timeStamp, operation, sender, amount0, amount1, amount0In, amount1In, amount0Out, amount1Out) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        ii = 0
        for d in data_list:
            self.cursor.execute(s1, (pair_id, d["transactionHash"], d["block_number"], d["timeStamp"],
                                     d["operation"], d["sender"],
                                     d.get("amount0"),
                                     d.get("amount1"),
                                     d.get("amount0In"),
                                     d.get("amount1In"),
                                     d.get("amount0Out"),
                                     d.get("amount1Out")))
            ii += 1
            if ii % 1000 == 0:
                self.db_connection.commit()

    def remove_event_history(self, pair_id):
        s1 = "delete t_event_history where pair_id=" + str(pair_id)
        self.cursor.execute(s1)
                                     
    def update_json(self, table, row_id, field, data, condition):
        for f in data:
            if type(data[f]) == float and np.isnan(data[f]):
                data[f] = ""
            if type(data[f]) == np.int64:
                data[f] = int(data[f])
            elif type(data[f]) == np.float64:
                data[f] = float(data[f])
        if type(row_id) == str:
            s1 = "UPDATE " + table + " SET " + field + "='" + json.dumps(data) + "' WHERE " + condition + "='" + str(row_id) + "'"
        else:
            s1 = "UPDATE " + table + " SET " + field + "='" + json.dumps(data) + "' WHERE " + condition + "=" + str(row_id)
        self.cursor.execute(s1)
        
    def fetch_with_description(self, cursor):
        return [{n[0]: v for n, v in zip(cursor.description, row)} for row in cursor.fetchall()]
        
    def get_pairs(self):
        s1 = "select pair_id, pair, token0, token1, token, swaps, first_block_number from t_pairs"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_pairs_max_block(self, pair_id_start=None, pair_id_end=None):
        s1 = "select t_pairs.pair_id pair_id, pair, token0, token1, token, swaps, "
        s1 += "first_block_number, max(t_event_history.block_number) max_block_number from t_pairs "
        s1 += "left join t_event_history on t_pairs.pair_id = t_event_history.pair_id "
        if not pair_id_start is None:
            s1 += " where t_pairs.pair_id >= " + str(pair_id_start)
            if not pair_id_end is None:
                s1 += " and t_pairs.pair_id < " + str(pair_id_end)
        elif not pair_id_end is None:
            s1 += " where t_pairs.pair_id < " + str(pair_id_end)
        s1 += " group by t_pairs.pair_id, pair, token0, token1, token, swaps, first_block_number"
        # print(s1)
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)
    
    def get_max_block_times(self, min_block=None):
        s1 = "select pair_id, max(timeStamp) max_block_timeStamp, max(block_number) max_block_number from t_event_history "
        if not min_block is None:
            s1 += " where block_number >= " + str(min_block)
        s1 += " group by pair_id"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)


    def get_pairs_no_text(self):
        s1 = "select pair_id, pair, token0, token1, token, swaps, first_block_number from t_pairs "
        s1 += "left join t_contract_code on t_pairs.token = t_contract_code.token_id "
        s1 += "where t_contract_code.token_id is null and not t_pairs.token is null"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_pairs_with_contracts(self, pair_id_start=None, pair_id_end=None, liquid_tokens=None):
        s1 = "select pair_id, pair, token0, token1, t_pairs.token token, t_tokens.decimals, swaps, first_block_number from t_pairs "
        s1 += "inner join t_contract_code on t_pairs.token = t_contract_code.token_id "
        s1 += "inner join t_tokens on t_pairs.token = t_tokens.token "
        s1 += "where length(t_contract_code.contract_text) > 20 "
        if not liquid_tokens is None:
            s1 += "and (token0 in (" + "".join([(", '" if i>0 else "'") + str(t) + "'" for i, t in enumerate(liquid_tokens)]) + ") "
            s1 += "or token1 in (" + "".join([(", '" if i>0 else "'") + str(t) + "'" for i, t in enumerate(liquid_tokens)]) + ")) "
            s1 += "and not t_pairs.token in (" + "".join([(", '" if i>0 else "'") + str(t) + "'" for i, t in enumerate(liquid_tokens)]) + ")"
        if not pair_id_start is None:
            s1 += " and pair_id >= " + str(pair_id_start)
        if not pair_id_end is None:
            s1 += " and pair_id < " + str(pair_id_end)
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_pair(self, pair_id):
        s1 = "select pair, token0, token1, token, swaps, first_block_number from t_pairs where pair_id = " + str(pair_id)
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_event_history(self, pair_id):
        s1 = "select * from t_event_history where pair_id = " + str(pair_id)
        self.cursor.execute(s1)
        history = self.fetch_with_description(self.cursor)
        for h in history:
            for f in h:
                if f[:6] == "amount":
                    if h[f] is None:
                        h[f] = 0
                    else:
                        h[f] = int(h[f])
        return history

    def get_event_history_many(self, pair_id_start, pair_id_end):
        s1 = "select * from t_event_history where pair_id >= " + str(pair_id_start) + " and pair_id < " + str(pair_id_end)
        self.cursor.execute(s1)
        history = self.fetch_with_description(self.cursor)
        for h in history:
            for f in h:
                if f[:6] == "amount":
                    if h[f] is None:
                        h[f] = 0
                    else:
                        h[f] = int(h[f])
        return history

    def get_json(self, table, row_id, field, condition):
        if type(row_id) == str:
            s1 = "select " + field + " from " + table + " WHERE " + condition + "='" + str(row_id) + "'"
        else:
            s1 = "select " + field + " from " + table + " WHERE " + condition + "=" + str(row_id)

        self.cursor.execute(s1)
        return json.loads(self.cursor.fetchall()[0][0])

    def add_contract_code(self, token, code):
        s1 = "DELETE FROM t_contract_code WHERE token_id = %s"
        self.cursor.execute(s1, (token, ))
        s2 = """INSERT INTO t_contract_code (token_id, contract_text) VALUES (%s, _binary "%s")"""
        self.cursor.execute(s2, (token, zlib.compress(pickle.dumps(code))))

    def check_contract_code(self, token):
        s1 = "SELECT length(contract_text) from t_contract_code where token_id = %s"
        n = self.cursor.execute(s1, (token, ))
        if n == 0:
            return -1
        else:
            (len_contract_text) = self.cursor.fetchone()
            if len_contract_text == 0:
                return 0
            else:
                return 1

    def get_contract_code(self, token):
        s1 = "SELECT contract_text, contract_analytics from t_contract_code where token_id = %s"
        n = self.cursor.execute(s1, (token, ))
        if n:
            (contract_text, contract_analytics) = self.cursor.fetchone()
            return token, pickle.loads(zlib.decompress(contract_text[1:-1])), contract_analytics
        else:
            return token, None, None

    def clean_for_reload(self, start_pair, end_pair):
        s1 = "delete from t_pairs where token is null and pair_id >= %s and pair_id < %s"
        s2 = "delete from t_pairs where not token is null and first_block_number is null and pair_id >= %s and pair_id < %s"
        s3 = "delete from t_pairs where (swaps is null or swaps = 0) and pair_id >= %s and pair_id < %s"
        
        i1 = self.cursor.execute(s1, (start_pair, end_pair))
        i2 = self.cursor.execute(s2, (start_pair, end_pair))
        i3 = self.cursor.execute(s3, (start_pair, end_pair))
        return i1, i2, i3

    def add_token(self, token, token_name, token_symbol, decimals):
        s0 = "select * from t_tokens where token=%s"
        l = self.cursor.execute(s0, (token, ))
        if l:
            s1 = "update t_tokens set token_name=%s, token_symbol=%s, decimals=%s where token = %s"
            self.cursor.execute(s1, (token_name[:255], token_symbol[:255], decimals, token))
        else:
            s1 = "insert into t_tokens(token, token_name, token_symbol, decimals) values(%s, %s, %s, %s)"
            self.cursor.execute(s1, (token, token_name[:255], token_symbol[:255], decimals))

    def get_token(self, token):
        s1 = "select token, token_name, token_symbol, decimals from t_tokens where token = '" + token + "'"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_tokens(self):
        s1 = "select token, token_name, token_symbol, decimals from t_tokens"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_tokens_with_property(self, property_name):
        s1 = "select token, token_name, token_symbol, decimals, JSON_EXTRACT(properties, '$.\"" + property_name + "\"') " + property_name + " from t_tokens where not JSON_EXTRACT(properties, '$.\"" + property_name + "\"') is null"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_tokens_without_property(self, property_name):
        s1 = "select token, token_name, token_symbol, decimals from t_tokens where JSON_EXTRACT(properties, '$.\"" + property_name + "\"') is null"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)
