# !/usr/bin/env python
# -*- coding:utf-8 -*-
""" 
@author: Wang Ye (Wayne)
@file: PipeLineConfig.py
@time: 2020/11/20
@contact: wangye@oppo.com
@site: 
@software: PyCharm
# code is far away from bugs.
"""

import os
import sys
sys.path.append('..')

from common_utils import *


def check_path(s: str):
    s = s.replace('\\', '/')
    if not s.endswith('/'):
        s += '/'
    if not os.path.exists(s):
        os.makedirs(s)
    return s


class PipeLineConfig:
    def __init__(self, **paths):
        log_root = check_path(paths['log_root'])
        result_root = check_path(paths['result_root'])
        self.save_raw_root = check_path(os.path.join(result_root, 'raw'))  # 解析log文件后的存储root,可提供给test工程使用
        self.save_aln_root = check_path(os.path.join(result_root, 'aln'))  # 数据对齐后的存储root
        self.save_pcs_root = check_path(os.path.join(result_root, 'pcs'))  # 数据对齐后的存储root
        self.save_hdf_root = check_path(os.path.join(result_root, 'hdf'))  # 存放HDF5文件的root

        sensor_map_2_to_3 = {
            'ACC': '1', 'GYRO': '2', 'MAG': '3', 'Orientation': '4', 'PRESSURE': '7', 'PPG': '31'
        }
        sensor_map_3_to_2 = {v: k for k, v in sensor_map_2_to_3.items()}

        self.params = {
            'key_words': {'ACC', 'GYRO', 'MAG', 'Orientation'},
            'align_anchor_sensor': 'ACC',
            'remove_last': {
                'ACC': 2, 'GYRO': 2, 'MAG': 2, 'Orientation': 2, 'PRESSURE': 2
            },  # 三代表无需设置
            'time_stamp_idx': {
                'ACC': 3, 'GYRO': 3, 'MAG': 3, 'Orientation': 4, 'PRESSURE': 1
            },  # 三代表无需设置
            'data_type_dict': sensor_map_2_to_3,
            'data_type_inv_dict': sensor_map_3_to_2,
            'freq': {
                'ACC': 100, 'GYRO': 100, 'MAG': 100, 'PRESSURE': 100, 'Orientation': 100
            },
            'max_missing_cnts': 100,
            'root': log_root,
            'save_raw_root': self.save_raw_root,
            'save_aln_root': self.save_aln_root,
            'save_pcs_root': self.save_pcs_root,
            'save_hdf_root': self.save_hdf_root,
            'fixed_shape': False,
            'in_root_hdf5': self.save_aln_root,
            'in_slice_root_hdf5': self.save_pcs_root,
            'save_hdf_path': os.path.join(self.save_hdf_root, 'result.hdf5'),
            'save_hdf_slice_path': os.path.join(self.save_hdf_root, 'result_slice.hdf5'),
            'data_view_path': os.path.join(self.save_hdf_root, 'result.hdf5'),
            'save_copy_in_the_same_dir': True,
        }
        self.params['min_interval'] = {
            k: 1e6 / v * 0.3 for k, v in self.params['freq'].items()
        }
        self.pipeline = PipeLine(**self.params)

    def config_pipeline(self, custom: dict, jobs):
        self.pipeline.update(custom)
        if DataSlice in jobs or self.params['fixed_shape']:
            self.pipeline.update({
                'fixed_shape': True,
                'data_view_path': os.path.join(self.save_hdf_root, 'result_slice.hdf5'),
            })
        else:
            self.pipeline.update({
                'fixed_shape': False,
            })
