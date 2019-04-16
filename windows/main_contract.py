# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
import datetime
import re
from PyQt5.QtWidgets import QMessageBox, QFileDialog
from PyQt5.QtCore import pyqtSignal

from utils.generate_time import GenerateTime
from windows.ancestor import AncestorThread
from windows.variety_price import VarietyPriceWindow
from utils.saver import get_desktop_path, open_excel


class MainContractWindow(VarietyPriceWindow):
    name = "main_contract"

    def confirm(self):
        """确认按钮点击"""
        # 开启定时器
        self.timer.start(1000)
        # 改变确认按钮的状态
        self.confirm_button.setText("提交成功")
        self.confirm_button.setEnabled(False)
        # 清除原图表并设置样式
        self.map_widget.delete()
        self.table_widget.clear()
        self.table_widget.set_style(header_labels=['日期', '价格', '成交量合计', '持仓量合计'])
        # 根据时间段取得时间生成器，查询相关数据，计算结果，返回时间列表及对应的价格列表
        lib = self.exchange_lib.currentText()
        variety = self.variety_lib.currentText()
        variety_en = self.get_variety_en(variety)
        begin_time = re.sub('-', '', str(self.begin_time.date().toPyDate()))
        end_time = re.sub('-', '', str(self.end_time.date().toPyDate()))
        print("确认提交:\n当前交易所：{}\n当前品种：{},英文{}\n起始时间：{}\n终止时间：{}\n".format(lib, variety, variety_en, begin_time, end_time))
        # 转成可计算的datetime.datetime时间对象
        begin_time_datetime_cls = datetime.datetime.strptime(begin_time, "%Y%m%d")
        end_time_datetime_cls = datetime.datetime.strptime(end_time, "%Y%m%d")
        # 起止时间应早于终止时间
        if begin_time_datetime_cls >= end_time_datetime_cls:
            QMessageBox.warning(self, "错误", "起止时间应早于终止时间", QMessageBox.Ok)
            self.message_signal.emit("时间选择有误！")
            self.timer.stop()
            self.cost_time = 0
            # 设置确认按钮可用
            self.confirm_button.setText("确定")
            self.confirm_button.setEnabled(True)
            return
        # 生成时间可迭代对象
        iterator = GenerateTime(begin=begin_time_datetime_cls, end=end_time_datetime_cls)
        print("执行线程")
        if self.db_worker:
            # 线程执行查询目标数据
            self.confirm_thread = ConfirmQueryThread(db_worker=self.db_worker, iterator=iterator, variety=variety_en, lib=lib)
            self.confirm_thread.result_signal.connect(self.draw_map_table)
            self.confirm_thread.process_signal.connect(self.show_process)
            self.confirm_thread.start()


class ConfirmQueryThread(AncestorThread):
    """确定查询线程"""
    result_signal = pyqtSignal(dict)

    def __init__(self, db_worker, variety, iterator, lib):
        super(ConfirmQueryThread, self).__init__()
        self.db_worker = db_worker
        self.variety = variety
        self.time_iterator = iterator.generate_time()
        self.time_length = iterator.length()
        self.lib = lib

    def run(self):
        """根据时间查询出相应的数据"""
        # (列表嵌套元组的方式返回, [("时间": "价格")]使得数据不会发生串位错乱)
        data = []
        for index, date in enumerate(self.time_iterator):
            data_couple = self.db_worker.main_contract_price(date=date, variety=self.variety)  # 方法返回一个元组
            if not data_couple:
                continue
            data.append(data_couple)
            # 设置进度条
            self.process_signal.emit(["处理进度:", index + 1, self.time_length])
        self.result_signal.emit({"data": data, "message": self.lib + self.variety + "主力合约价格指数"})
        self.db_worker.close()
