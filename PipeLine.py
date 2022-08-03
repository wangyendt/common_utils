# !/usr/bin/env python
# -*- coding:utf-8 -*-
""" 
@author: Wang Ye (Wayne)
@file: PipeLine.py
@time: 2020/11/20
@contact: wangye@oppo.com
@site: 
@software: PyCharm
# code is far away from bugs.
"""


class PipeLine:
    def __init__(self, **params):
        self.params = params

    def update(self, params):
        self.params.update(params)

    def run(self, jobs: list):
        for job in jobs:
            hr = f'{job.__name__}_getter'
            if hr in self.params:
                job = self.params[hr]
            j = job(**self.params)
            j()
