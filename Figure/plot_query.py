import numpy as np
import matplotlib.pyplot as plt
import easypyplot
import pandas as pd
import matplotlib.font_manager as fm

# Insert Op
works_list = ['FAST&FAIR', 'LB+Trees', 'WORT', 'WOART', 'ROART', 'ERT']

read_csv = pd.read_csv('../Result/query.csv')
data_2darr = []
wl_list = ['Dense', 'Sparse']
for wl in wl_list:
    data_2darr.append(np.array(read_csv[wl]))

group_name = works_list
fig_dims = (5, 1.5)
fig_name = '{}'.format("query")
pp, fig = easypyplot.pdf.plot_setup(fig_name, fig_dims)
ax = fig.gca()
easypyplot.format.turn_off_box(ax)
# x ticks
group_xticks = []
xtick_beg = 0
color_item = [easypyplot.color.COLOR_SET[i] for i in [4, 6, 1, 2, 3, 0, 5]]
hdls = []
for idx, hitrate in enumerate(data_2darr):
    group_xticks.append(xtick_beg)
    xtick_beg += 1

bar_width = 0.7
hdls = easypyplot.barchart.draw(ax, data_2darr, width=bar_width, breakdown=False, xticks=group_xticks, group_names=wl_list, colors=color_item)
ax.set_xticklabels([], fontsize=8)
ax.set_xlim([ax.get_xticks()[0] - 1, ax.get_xticks()[-1] + 1])
ax.xaxis.set_ticks_position('none')

# y axis
ax.yaxis.grid(True)
ax.set_ylabel('Query Throughput (Mops)')

fig.tight_layout()
easypyplot.format.resize_ax_box(ax, hratio=1)
easypyplot.format.resize_ax_box(ax, wratio=0.75)

# workload text
name_y_pos = -0.05
for idx, case in enumerate(wl_list):
    x = ax.get_xticks()[idx] + 0.15
    ax.text(x, name_y_pos, case, ha='right', va='top', fontsize=9)

# Create legend
ax.legend(hdls, group_name, frameon=False, bbox_to_anchor=(1, 1.05), loc='upper left', ncol=1)
easypyplot.pdf.plot_teardown(pp)