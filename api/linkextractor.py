#!/usr/bin/env python

from flask import Flask, jsonify
import pandas as pd




def columnas(df , a1 ,a2 ,x):
    # Group by 'userId' and 'movieId' and calculate the mean of 'rating'
    #consolidated_df1 = df.groupby([a1, a2])[x].mean().unstack()
    consolidated_df1 = df.groupby([a1, a2])[x].mean().unstack(fill_value=0)
    return consolidated_df1



