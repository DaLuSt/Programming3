"""
Assignment 3 Programming 3
Data Sciences for Life Sciences
Author: Daan Steur
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

data = pd.read_csv('timings.txt', sep="", header=None)
y = data.time
x = np.linspace(1, 17)
plt.plot(x, y)

# load timings.txt
# export timings.png

        
        
    

        














# command line assignment3.py –hosts <list of hosts, first is server> [-s | -c] -n cores -p port -d jobsdirectory -r <retries>