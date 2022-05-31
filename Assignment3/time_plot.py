"""
Assignment 3 Programming 3
Data Sciences for Life Sciences
Author: Daan Steur
"""

import matplotlib.pyplot as plt

with open('output/time.txt') as f:
    lines = f.readlines()

thread_list = []
time_list = []

for line in lines:
    print(line)
    if (line.startswith('threads')):
        thread_list.append(int(line.split(' ')[1].split('\n')[0]))

    if (line.startswith('real')):
        time = ((line.split('	')[1].split('\n')[0]))
        time = float(time.split('m')[0]) * 60 + float(time.split('m')[1].split('s')[0])
        time_list.append(float(time))

print(thread_list)
print(time_list)

plt.bar(thread_list, time_list, color='b', width = 1)
plt.xlabel('CPU threads')
plt.ylabel('Time (s)')
plt.title('Time taken to run the program')
plt.savefig('output/time_plot.png')
        
        
    

        














# command line assignment3.py –hosts <list of hosts, first is server> [-s | -c] -n cores -p port -d jobsdirectory -r <retries>