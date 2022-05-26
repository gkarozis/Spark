import matplotlib
import matplotlib.pyplot as plt
import numpy as np


labels = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']
rdd_means = [25.51, 114.53, 81.795, 10.418, 343.31]
sql_means = [27.64, 94.058, 70.27, 24.27, 337.79]
parquet_means = [26.094, 35.25, 25.444, 20.043, 0]

x = np.arange(len(labels))  # the label locations
width = 0.25  # the width of the bars
print(x)
fig, ax = plt.subplots()
rects1 = ax.bar(x +0, rdd_means, width, label='RDD')
rects2 = ax.bar(x +0.25, sql_means, width, label='sql')
rects3 = ax.bar(x + 0.5, parquet_means, width, label='parquet')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Time in s')
ax.set_title('Time by query')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()


def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


autolabel(rects1)
autolabel(rects2)
autolabel(rects3)

fig.tight_layout()

plt.show()