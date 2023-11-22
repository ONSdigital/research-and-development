# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 11:46:52 2023
harmonise lengths
@author: zoring

"""
import pandas as pd

mydata = {"pc": ["A B", "A B C", "1234 567", "12345678", "123456789"]}
df = pd.DataFrame(mydata)


target_len = 8
_topup
df['pc'] = df['pc'].apply(_topup)