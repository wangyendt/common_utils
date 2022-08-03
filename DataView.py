# !/usr/bin/env python
# -*- coding:utf-8 -*-
""" 
@author: Wang Ye (Wayne)
@file: DataView.py
@time: 2020/11/19
@contact: wangye@oppo.com
@site: 
@software: PyCharm
# code is far away from bugs.
"""

import logging
import math
import os
import random
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pywayne.tools import maximize_figure

logger = logging.getLogger('main.DataView')


class DataView:
    def __init__(self, **params):
        self.kws = list(sorted(params['key_words']))
        self.view_path = params['data_view_path']
        self.shuffle = params['plot_random_file']
        self.x = None
        self.x_size = 0
        self.fig = None
        self.ax = []

    @maximize_figure
    def _plot(self, df: pd.DataFrame, file: str):
        x = df['time_stamp'] / 10 ** 6
        x -= x[0]
        self.x = x
        self.x_size = len(x)
        n = len(self.kws)
        r = math.floor(np.sqrt(n)) + 1
        c = n // r if n % r == 0 else n // r + 1
        self.fig, self.ax = fig, ax = plt.subplots(r, c, sharex='all')
        ax = ax.T.flatten()
        for kid, kw in enumerate(self.kws):
            d = df.filter(regex=kw)
            ax[kid].plot(x, d, '.-', markersize=8)
            ax[kid].legend(d.columns, loc=1)
            ax[kid].set_xlabel('Time: (s)')
        [ax[i].grid(True) for i in range(len(ax))]
        fig.suptitle(file)

        return fig

    def __call__(self):
        start_time = time.time()
        while not os.path.exists(self.view_path):
            if time.time() - start_time > 5:
                logger.error('HDF5 file not found, waiting timeout!')
                return
            time.sleep(0.3)

        with pd.HDFStore(self.view_path, 'r') as store:
            counts = len(store.keys())
            indices = range(counts)
            if self.shuffle:
                indices = random.sample(indices, counts)
            for idx in indices:
                key = f'data/id_{idx}'
                if key in store:
                    df = store[key]
                    file = store.get_storer(f'data/id_{idx}').attrs.metadata
                    self._plot(df, file)
                    plt.show()
                else:
                    print(f'{key} not found in file {self.view_path}')
