# !/usr/bin/env python
# -*- coding:utf-8 -*-
""" 
@author: Wang Ye (Wayne)
@file: DataSlice.py
@time: 2020/11/20
@contact: wangye@oppo.com
@site: 
@software: PyCharm
# code is far away from bugs.
"""

import logging
import os
import shutil

import pandas as pd
from pywayne.tools import list_all_files

logger = logging.getLogger('main.DataSlice')


class DataSlice:
    def __init__(self, **params):
        self._slice_func = params['slice_func']
        self._in_root = params['save_aln_root']
        self._out_root = params['save_pcs_root']

    def __call__(self):

        logger.info('--- Start slicing aligned .aln data to .pcs data ---')

        if os.path.exists(self._out_root):
            shutil.rmtree(self._out_root)
        while True:
            try:
                os.makedirs(self._out_root)
                break
            except:
                logger.debug(f'Cannot create directory: {self._out_root}')
        files = list_all_files(self._in_root, ['.aln'])
        logger.info(f'{len(files)} files to be sliced!')
        for fid, file in enumerate(files):
            logger.info(f'Slicing data: {fid + 1}/{len(files)}')
            df = pd.read_csv(file, encoding='utf-8')
            for idx, df_res in enumerate(self._slice_func(df)):
                df_res.to_csv(
                    os.path.join(self._out_root, os.path.basename(file)).replace(
                        '.aln', f'_[pcs_{idx + 1}].pcs'
                    ),
                    index=False,
                    encoding='utf-8'
                )

        logger.info('--- Complete slicing aligned .aln data to .pcs data ---')
