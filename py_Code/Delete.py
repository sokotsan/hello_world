# -*- coding: utf-8 -*-
"""
Created on Fri May 10 13:25:24 2019

@author: E204775
"""

import os
#%%

in_path='D:\\DASH\\'


#%%
#Delete existing files prior do download
# =============================================================================
test=os.listdir(in_path)
print(test)
for item in test:
     if item.endswith("*TRNS*.csv"):
         os.remove(os.path.join( in_path, item ) )


        

