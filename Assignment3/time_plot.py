"""
Assignment 3 Programming 3
Data Sciences for Life Sciences
Author: Daan Steur
"""
import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv(
    "/output/timings.txt",
    sep="\t",
    lineterminator="\n",
    header=None
)
x = data.iloc[:, 0]
y = data.iloc[:, 1]
plt.scatter(x, y)
plt.plot(x, y)
plt.title("Blastp runtime with variating thread count")
plt.xlabel("Number of threads")
plt.ylabel("Time taken (s)")
plt.savefig("output/timings.png")
        
# command line assignment3.py –hosts <list of hosts, first is server> [-s | -c] -n cores -p port -d jobsdirectory -r <retries> 