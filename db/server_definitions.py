#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class Servers():

    aws_215_mysql = {"ssh_host": "10.0.1.215",
                       "ssh_port": 22,
                       "ssh_username": "ubuntu",
                       "ssh_pkey": "~/.ssh/id_rsa",
                       "local_bind_address": None,
                       "bind_address": '127.0.0.1',
                       "bind_port": 3306,
                       "SSH_TIMEOUT": 30.0}

    rsynergy2_mysqlconnect = {"ssh_host": "ramses.mil.r-synergy.com",
                       "ssh_port": 22,
                       "ssh_username": "sqlconnect",
                       "ssh_pkey": "~/.ssh/id_rsa",
                       "local_bind_address": None,
                       "bind_address": '127.0.0.1',
                       "bind_port": 3306,
                       "SSH_TIMEOUT": 30.0}

