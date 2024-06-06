#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
UNISWAP_V2_FEE = 0.003

V2_FACTORY = {"UNISWAP_V2_FACTORY": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".lower(), "SUSHI_FACTORY": "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac".lower()}
V3_FACTORY = {"V3_FACTORY": "0x1F98431c8aD98523631AE4a59f267346ea31F984".lower()}
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()

def extract_path_from_V3(str_path):
    path = []
    i = 0
    while i < len(str_path):
        path.append(("0x" + str_path[i:i+20].hex().lower(), str_path[i+20:i+23]))
        i = i + 23
    return path

def amount_out_v2(x, x0, y0):
    # if (x0 + x * (1 - UNISWAP_V2_FEE)) == 0:
    #     return 0
    # else:
    return y0 * x * (1 - UNISWAP_V2_FEE) / (x0 + x * (1 - UNISWAP_V2_FEE))

def optimal_amount_formula(X0, Y0, Xv, Yv):
    return (-Yv * (1997000 * X0 + 994009 * Xv)
              + np.sqrt(Yv * (9000000 * X0**2 * Yv
                             + 3976036000000 * X0 * Xv * Y0
                             - 5964054000 * X0 * Xv * Yv
                             + 988053892081 * Xv**2 * Yv)
                    )
            ) / (1994000 * Yv)

def profit_function(Xa, X0, Y0, Xv):
    Ya = amount_out_v2(Xa, X0, Y0)
    
    X1 = X0 + Xa
    Y1 = Y0 - Ya

    Yv = amount_out_v2(Xv, X1, Y1)

    X2 = X1 + Xv
    Y2 = Y1 - Yv

    return amount_out_v2(Ya, Y2, X2) - Xa    
