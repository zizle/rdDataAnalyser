# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle

import time


def cost_time(func):
    def call_func(*args, **kwargs):
        start = time.time()
        data = func(*args, **kwargs)
        end = time.time()
        print("花费时间:", end - start)
        return data
    return call_func
