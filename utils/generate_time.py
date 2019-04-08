# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle

import datetime


class GenerateTime(object):
    def __init__(self, begin, end):
        self.begin = begin
        self.end = end

    def generate_time(self):
        """生成可迭代的时间区间"""
        while self.begin <= self.end:
            # 将date转为时间字符串
            yield datetime.datetime.strftime(self.begin, '%Y%m%d')
            # 日期+1天
            self.begin += datetime.timedelta(days=1)

    def length(self):
        return (self.end - self.begin).days


