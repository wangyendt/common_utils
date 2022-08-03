# !/usr/bin/env python
# -*- coding:utf-8 -*-
""" 
@author: Wang Ye (Wayne)
@file: DataAlign.py
@time: 2020/11/18
@contact: wangye@oppo.com
@site: 
@software: PyCharm
# code is far away from bugs.
"""

import json
import logging
import os
import shutil
import sys

import numpy as np
import pandas as pd
from pywayne.tools import count_file_lines, list_all_files

logger = logging.getLogger('main.DataAlign')


class DataAlign:
    def __init__(self, **params):
        self._root = params['root']
        self._in_root = params['save_raw_root']
        self._out_root = params['save_aln_root']
        self._kws = params['key_words']
        self._anchor_sensor = params['align_anchor_sensor']
        self._data_type_dict = params['data_type_dict']
        self._data_type_inv_dict = params['data_type_inv_dict']
        self._min_interval = params['min_interval']
        self._max_missing_cnts = params['max_missing_cnts']
        self._freq_dict = params['freq']
        self._save_copy_in_the_same_dir = params['save_copy_in_the_same_dir']
        self._cur_data = dict()
        self._interval = dict()
        self._prepare()

    def _prepare(self):
        for k, v in self._freq_dict.items():
            self._cur_data[k] = dict()
            self._interval[k] = 1_000_000 / v

    def _process(self, file) -> dict:
        with open(file, 'r', encoding='utf-8') as f:
            total = count_file_lines(file)
            cnt = 0
            last_time_stamp = {kw: -sys.maxsize for kw in self._kws}
            is_filled = set()
            b_init_flag = False
            acc_cur_ts = 0
            while True:
                line = f.readline()
                if not line: break
                if (cnt + 1) % 100000 == 0:
                    logger.debug(f'[ANALYZE] {cnt + 1}/{total} is done, please wait!')
                cnt += 1
                line = line.rstrip('\n').rstrip('\t')
                pieces = line.split('\t')
                data_type = pieces[0]
                time_stamp = int(pieces[-1][:-3])  # 截断保留微秒数为单位传给算法
                kw = self._data_type_inv_dict[data_type]
                if time_stamp < last_time_stamp[kw] + self._min_interval[kw]:
                    continue
                last_time_stamp[kw] = time_stamp
                data = pieces[1:-1]

                data = np.array(data, dtype=np.float)
                now_time_stamp = time_stamp
                if kw == self._anchor_sensor:
                    now_acc = data
                    if not b_init_flag:
                        acc_cur_ts = now_time_stamp
                        self._cur_data[self._anchor_sensor] = data
                        b_init_flag = True
                    if self._kws == is_filled | {self._anchor_sensor}:
                        miss_cnt = 0
                        while True:
                            if miss_cnt == self._max_missing_cnts:
                                logger.error('missing data!! ###############')
                                acc_cur_ts = now_time_stamp
                                self._cur_data[self._anchor_sensor] = now_acc
                                break
                            miss_cnt += 1
                            next_time_stamp = acc_cur_ts + self._interval[self._anchor_sensor]
                            if now_time_stamp >= next_time_stamp:
                                w = (next_time_stamp - acc_cur_ts) / (now_time_stamp - acc_cur_ts)
                                cur_acc = self._cur_data[self._anchor_sensor]
                                self._cur_data[self._anchor_sensor] = cur_acc * (1 - w) + now_acc * w
                                acc_cur_ts = next_time_stamp
                                yield {
                                    'data': self._cur_data,
                                    'time_stamp': next_time_stamp,
                                }
                            else:
                                break
                else:
                    self._cur_data[kw] = data
                    is_filled.add(kw)

    def __call__(self):

        logger.info('--- Start aligning .raw data to .aln data ---')

        if os.path.exists(self._out_root):
            shutil.rmtree(self._out_root)
        while True:
            try:
                os.makedirs(self._out_root)
                break
            except:
                logger.debug(f'Cannot create directory: {self._out_root}')

        raw_log_map_path = 'log/raw_log_map.json'
        with open(raw_log_map_path, 'r', encoding='gbk') as j:
            raw_log_map = json.load(j)
        files = list_all_files(self._in_root, ['.raw'])
        logger.info(f'{len(files)} files to be aligned!')
        for fid, file in enumerate(files):
            logger.info(f'Aligning data: {fid + 1}/{len(files)}')
            df_dict = dict()
            for idx, data in enumerate(self._process(file)):
                if (idx + 1) % 100000 == 0:
                    logger.debug(f'Aligning data, please wait...')
                df_dict[idx] = dict()
                df_dict[idx]['time_stamp'] = data['time_stamp']
                for k, v in data['data'].items():
                    for vi, vv in enumerate(v):
                        df_dict[idx][f'{k}_{vi + 1}'] = vv
            df = pd.DataFrame.from_dict(df_dict).T
            out_file = os.path.join(self._out_root, os.path.basename(file).replace('.raw', '.aln'))
            df.to_csv(
                out_file,
                index=False,
                encoding='utf-8'
            )
            if self._save_copy_in_the_same_dir:
                shutil.copy(out_file, raw_log_map[file].replace('.log', '.aln'))

        logger.info('--- Complete aligning .raw data to .aln data ---')
