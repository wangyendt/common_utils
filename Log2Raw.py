# !/usr/bin/env python
# -*- coding:utf-8 -*-
""" 
@author: Wang Ye (Wayne)
@file: Log2Raw.py
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

from pywayne.tools import count_file_lines
from pywayne.tools import list_all_files

logger = logging.getLogger('main.Log2Raw')


class Log2Raw:
    def __init__(self, **params):
        self._root = params['root']
        self._save_raw_root = params['save_raw_root']
        self._data_type_dict = params['data_type_dict']
        self._data_type_inv_dict = params['data_type_inv_dict']
        self._kws = params['key_words']
        self._remove_last = params['remove_last']
        self._time_stamp_idx = params['time_stamp_idx']
        self._sort_rawdata_by_timestamp = params['sort_rawdata_by_timestamp']
        self._save_copy_in_the_same_dir = params['save_copy_in_the_same_dir']
        if 'logger' in params:
            global logger
            logger = params['logger']

    @staticmethod
    def _clear_dirs(dir_path: str):
        if input(f'Are you sure to delete {dir_path} ? (Y/N)') in 'Yy':
            logger.debug(f'Clear path: {dir_path}')
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
            os.makedirs(dir_path)

    @staticmethod
    def _get_dst_path_from_src_path(_dst_root: str, _src_root: str, _src_path: str, _src_ext: str, _dst_ext: str):
        return _dst_root + _src_path[len(_src_root):].replace('/', '_').replace('\\', '_').replace(_src_ext, _dst_ext)

    def _transfer_log_to_raw_data(self, in_file: str, out_file: str):
        def write_file(s_: str, f_: str):
            with open(f_, 'a') as writer:
                writer.writelines(s_)

        mode = 'v2'
        with open(in_file, 'r') as f:
            line = f.readline()
            if line and ':' not in line:
                mode = 'v3'

        with open(in_file, 'r') as f:
            total = count_file_lines(in_file)
            save_list = []
            while True:
                line = f.readline().rstrip('\n')
                if not line: break
                if mode == 'v2':
                    data_piece = line[13:].lstrip('\t').split(':')
                    data_type = data_piece[0]  # ACC,PRESSURE
                    if data_type not in self._kws: continue
                    data = data_piece[1].split(',')
                    saver = ''
                    saver += f'{self._data_type_dict[data_type]}\t'
                    for d in data[:-self._remove_last[data_type]]:
                        saver += f'{d}\t'
                    ts = data[self._time_stamp_idx[data_type]]
                    saver += ts + '\t'
                    saver += '\n'
                elif mode == 'v3':
                    items = line.split(',')
                    data_type, data, time, utc = items[0], items[1:-2], items[-2], items[-1]
                    data_type = self._data_type_inv_dict[data_type]
                    if data_type not in self._kws: continue
                    saver = ''
                    saver += f'{self._data_type_dict[data_type]}\t'
                    for d in data:
                        saver += f'{d}\t'
                    ts = time
                    saver += ts + '\t'
                    saver += '\n'
                save_list.append((int(ts), saver))

            if self._sort_rawdata_by_timestamp:
                save_list = sorted(save_list, key=lambda x: x[0])
            saver = ''
            for cnt, (_, save_item) in enumerate(save_list):
                saver += save_item
                if (cnt + 1) % 100000 == 0:
                    write_file(saver, out_file)
                    saver = ''
                    logger.debug(f'[TRANSFER] {cnt + 1}/{total} is done, please wait!')
            write_file(saver, out_file)

    def __call__(self):

        logger.info('--- Start transferring .log data to .raw data ---')

        csv_files = list_all_files(root=self._root, keys_and=['.csv'])
        for c_file in csv_files:
            shutil.copy(c_file, c_file.replace('.csv', '.log'))

        raw_log_files = list_all_files(root=self._root, keys_and=['.log'], outliers=['.bat', 'mcu_log', '.nmea', 'btsnoop_hci'])
        if not os.path.exists('log'):
            os.makedirs('log')
        registered_file_path = 'log/registered files.txt'
        reg_files = []

        self._clear_dirs(self._save_raw_root)
        if os.path.exists(registered_file_path):
            os.remove(registered_file_path)

        if os.path.exists(registered_file_path):
            logger.info('--- Start checking all .raw files in registered file ---')
            raw_file_checker = set()
            with open(registered_file_path, 'r', encoding='utf-8') as f:
                files = f.readlines()
                for file_i, file in enumerate(files):
                    file = file.rstrip('\n')
                    if (os.path.exists(file) and
                            os.path.exists(self._get_dst_path_from_src_path(self._save_raw_root, self._root, file, '.log', '.raw'))):
                        raw_file_checker.add(file.replace('\\', '/') + '\n')
                        logger.debug(f'Valid path ({file_i + 1}/{len(files)}) in registered_files.txt, saving: {file}')
                    else:
                        logger.debug(f'Invalid path ({file_i + 1}/{len(files)}) in registered_files.txt, deleting: {file}')

            os.remove(registered_file_path)
            with open(registered_file_path, 'w+', encoding='utf-8') as f:
                f.writelines(''.join(list(sorted(raw_file_checker))))
            logger.info('--- Complete checking all .raw files in registered file ---')

            with open(registered_file_path, 'r', encoding='utf-8') as f:
                while True:
                    line = f.readline()
                    if line:
                        reg_files.append(line.strip('\n'))
                    else:
                        break

        logger.info(f'{len(raw_log_files)} files to be transferred!')

        raw_log_map_path = 'log/raw_log_map.json'
        raw_file_paths_dict = {}
        if os.path.exists(raw_log_map_path):
            os.remove(raw_log_map_path)

        for idx, file in enumerate(raw_log_files):
            if file.replace('\\', '/') in reg_files:
                logger.debug(f'.log file has already been registered: {file}')
                continue
            else:
                save_raw_path = self._get_dst_path_from_src_path(self._save_raw_root, self._root, file, '.log', '.raw')
                logger.info(f'Transferring .log data to .raw data ({idx + 1}/{len(raw_log_files)})')
                # logger.debug(f'Transferring file from: {file}')
                # logger.debug(f'Transferring file to: {save_raw_path}')
                self._transfer_log_to_raw_data(file, save_raw_path)
                if os.path.exists(save_raw_path):
                    with open(registered_file_path, 'a', encoding='utf-8') as f:
                        f.writelines(file.replace('\\', '/') + '\n')
                if self._save_copy_in_the_same_dir:
                    shutil.copy(save_raw_path, file.replace('.log', '.raw'))
                    raw_file_paths_dict.update({save_raw_path: file.replace('\\', '/')})
        with open(raw_log_map_path, 'w', encoding='gbk') as j:
            json.dump(raw_file_paths_dict, j, ensure_ascii=False)

        logger.info('--- Complete transferring .log data to .raw data ---')
