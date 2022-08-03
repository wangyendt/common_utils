# !/usr/bin/env python
# -*- coding:utf-8 -*-
""" 
@author: Wang Ye (Wayne)
@file: HDF5Transfer.py
@time: 2020/11/20
@contact: wangye@oppo.com
@site: 
@software: PyCharm
# code is far away from bugs.
"""

import logging
import os

import h5py
import pandas as pd

from pywayne.tools import list_all_files

logger = logging.getLogger('main.HDF5Transfer')


class HDF5Transfer:
    def __init__(self, **params):
        self._in_root = params['in_root_hdf5']
        self._in_slice_root = params['in_slice_root_hdf5']
        self._save_hdf_path = params['save_hdf_path']
        self._save_hdf_slice_path = params['save_hdf_slice_path']
        self._fixed_shape = params['fixed_shape']
        self._trans_func = params['hdf5_trans_func']

    def __call__(self):

        logger.info('--- Start packing .pcs data to .hdf5 data ---')

        if self._fixed_shape:
            files = list_all_files(self._in_slice_root)
            if os.path.exists(self._save_hdf_slice_path):
                os.remove(self._save_hdf_slice_path)
        else:
            files = list_all_files(self._in_root)
            if os.path.exists(self._save_hdf_path):
                os.remove(self._save_hdf_path)

        n_examples = len(files)

        logger.info(f'{n_examples} files to be saved to .hdf5!')

        if n_examples == 0:
            return

        for fid, file in enumerate(files):
            res = self._trans_func(file)
            if not res: continue
            if self._fixed_shape:  # data has been sliced
                with h5py.File(self._save_hdf_slice_path, 'a') as f:
                    if fid == 0:
                        example_shape = res['data'].shape
                        f.create_dataset('data', shape=(n_examples,) + example_shape)
                        f.create_dataset('label', shape=(n_examples, 1))
                        f.create_dataset('file', shape=(n_examples, 1), dtype=h5py.special_dtype(vlen=str))
                    x_dataset = f['data']
                    y_dataset = f['label']
                    f_dataset = f['file']
                    if res['data'].shape == example_shape:
                        x_dataset[fid, :, :] = res['data'].values
                        y_dataset[fid] = res['label']
                        f_dataset[fid] = res['file']
            else:
                with pd.HDFStore(self._save_hdf_path, 'a') as store:
                    store[f'data/id_{fid}'] = res['data']
                    store.get_storer(f'data/id_{fid}').attrs.metadata = res['file']

        logger.info('--- Complete packing .pcs data to .hdf5 data ---')


if __name__ == '__main__':
    # with h5py.File(r'D:\Work\github\dataset\mileage\result\hdf\result.hdf5', 'r') as f:
    #     print(f.keys(), 'x' in f)
    #     print(f['x']['id_0'], len(f['x']))
    # data = pd.read_hdf(
    #     r'D:\Work\github\dataset\mileage\result\hdf\result.hdf5',
    #     'x/id_0', 'r'
    # )

    with pd.HDFStore(r'D:\Work\github\dataset\mileage\result\hdf\result.hdf5', 'r') as store:
        print(store.keys())
