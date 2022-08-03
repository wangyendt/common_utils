# !/usr/bin/env python
# -*- coding:utf-8 -*-
""" 
@author: Wang Ye (Wayne)
@file: example.py
@time: 2020/11/20
@contact: wangye@oppo.com
@site: 
@software: PyCharm
# code is far away from bugs.
"""

import os

from PipeLineConfig import *
import pandas as pd
import easygui


def my_slice_func(x: pd.DataFrame) -> pd.DataFrame:
    yield x.iloc[:100]


def my_hdf5_trans_func(file: str):
    if os.path.getsize(file) < 2 * 1024:
        print(f'文件太小: {file}')
        return None
    else:
        return {
            'data': pd.read_csv(file, encoding='utf-8'),
            'label': 'L' in file,  # 自定义标签
            'file': file,
        }


def main():
    custom = {
        'slice_func': my_slice_func,
        'hdf5_trans_func': my_hdf5_trans_func,
        'plot_random_file': True,
        'sort_rawdata_by_timestamp': False
    }
    jobs = [
        Log2Raw,  # sensorlog文件去除文本信息转化为数字信息
        DataAlign,  # 数据对齐，输出矩阵格式
        # DataSlice,  # 数据切分
        HDF5Transfer,  # 大量文件数据合并在一个hdf5文件
        DataView,  # 随机画图形波形，便于观察波形特征
        # MileageDataView,  # DataView的子类，用户手动截取室内里程有效数据
    ]
    config = PipeLineConfig(**paths)
    config.config_pipeline(custom, jobs)
    pipeline = config.pipeline
    pipeline.run(jobs)


if __name__ == '__main__':

    import logging
    import sys

    logger = logging.getLogger('main')
    logger.propagate = 0  # To avoid message to be propagated to upper level.
    logger.setLevel(level=logging.DEBUG)

    formatter = logging.Formatter(f'%(asctime)s-%(module)s-line[%(lineno)d]-%(levelname)s-%(message)s')

    # StreamHandler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level=logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    root = easygui.diropenbox(msg='请选择存储数据的文件夹根目录：') or r'D:\work\data\phone_data\imu_sensor_fusion\data'
    paths = dict()
    paths['log_root'] = os.path.join(root, 'log')
    paths['result_root'] = os.path.join(root, 'res')
    main()
