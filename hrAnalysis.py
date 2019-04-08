# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
"""项目入口文件"""

import sys

from PyQt5.QtWidgets import QApplication
from windows.master import HRMainWindow
app = QApplication(sys.argv)
cp_window = HRMainWindow()
cp_window.show()
sys.exit(app.exec_())
