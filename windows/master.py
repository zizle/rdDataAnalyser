# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
import xlwt
import datetime
import win32com.client
from win32com.shell import shell, shellcon

from PyQt5.QtWidgets import (QMainWindow, QAction,
    QLabel, QStatusBar, QStackedWidget,
    QProgressBar, QMessageBox, QFileDialog
)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon,QFont, QPalette

from settings import VERSION
from windows.net_holding import NetHoldingWindow
from windows.variety_price import VarietyPriceWindow
from windows.main_contract import MainContractWindow
from windows.sys_window import SysWindow


class HRMainWindow(QMainWindow):
    def __init__(self):
        super(HRMainWindow, self).__init__()
        self.setWindowTitle("瑞达期货研究院持仓研究系统 " + VERSION)
        self.setWindowIcon(QIcon('static/Icon.png'))
        main_menu = self.menuBar()
        """主菜单"""
        # 功能选择
        active_menu = main_menu.addMenu('功能选择')
        # 系统设置
        system_menu = main_menu.addMenu('系统设置')
        # 导出数据
        export_data = main_menu.addMenu('导出数据')

        """子菜单"""
        # 价格-净持率
        price_holding = QAction("价格 - 净持率", self)
        price_holding.setStatusTip('生成指定时间价格和净持率的走势图')
        price_holding.triggered.connect(self.price_holding)
        # 品种-权重
        variety_percent = QAction("品种权重指数", self)
        variety_percent.setStatusTip('品种的在存续合约持仓权重生成指数')
        variety_percent.triggered.connect(self.variety_percent)
        # 主力合约
        main_contract = QAction("主力合约指数", self)
        main_contract.setStatusTip('最大持仓品种的历史价格指数')
        main_contract.triggered.connect(self.main_contract)
        # 一键更新子菜单
        one_key_update = QAction('一键更新系统数据', self)
        one_key_update.setStatusTip('一键更新四大交易所的所有数据')
        one_key_update.triggered.connect(self.one_key_update)
        # 指定更新子菜单
        point_key_update = QAction("指定更新系统数据", self)
        point_key_update.setStatusTip("选择交易所更新数据")
        point_key_update.triggered.connect(self.point_key_update)
        # 添加品种子菜单
        add_variety = QAction("指定添加新的品种", self)
        add_variety.setStatusTip("选择交易所添加新品种")
        add_variety.triggered.connect(self.add_variety)
        # 查询当前最新数据时间
        view_lasted = QAction("查看最新数据时间", self)
        view_lasted.setStatusTip("查看当前系统中数据的最新时间")
        view_lasted.triggered.connect(self.view_lasted)
        # 更新品种权重价格指数菜单
        variety_price_btn = QAction("更新品种价格指数", self)
        variety_price_btn.setStatusTip("更新品种价格指数数据库")
        variety_price_btn.triggered.connect(self.update_price_index)

        # 导出Excel
        export_excel = QAction('导出Excel', self)
        export_excel.setStatusTip("导出数据到Excel文件")
        export_excel.triggered.connect(self.export_excel)
        """将子菜单添加到父菜单"""
        # 功能菜单
        active_menu.addAction(price_holding)
        active_menu.addAction(variety_percent)
        active_menu.addAction(main_contract)
        # 系统菜单
        system_menu.addAction(one_key_update)
        system_menu.addAction(point_key_update)
        system_menu.addAction(add_variety)
        system_menu.addAction(view_lasted)
        system_menu.addAction(variety_price_btn)
        # 导出数据菜单
        export_data.addAction(export_excel)

        # 中心提示显示-首页展示
        attention_label = QLabel("请点击左上角'功能选择'菜单\n选择您要的功能选项")
        attention_label.setAlignment(Qt.AlignCenter)
        attention_label.setFont(QFont("SimHei", 11, QFont.Bold))
        pe = QPalette()
        pe.setColor(QPalette.WindowText, Qt.red)
        attention_label.setPalette(pe)
        """中心窗口堆栈"""
        self.window_stack = QStackedWidget()
        self.net_holding_window = NetHoldingWindow(title='品种净持率-价格走势')
        self.variety_price_window = VarietyPriceWindow(title='品种权重指数')
        self.main_contract_window = MainContractWindow(title='主力合约历史价格指数')
        self.window_stack.addWidget(attention_label)
        self.window_stack.addWidget(self.net_holding_window)
        self.window_stack.addWidget(self.variety_price_window)
        self.window_stack.addWidget(self.main_contract_window)
        self.setCentralWidget(self.window_stack)

        """初始化系统隐藏窗口，用于执行系统相关操作"""
        self.sys_window = SysWindow(title="系统窗口")
        """状态栏"""
        self.status_bar = QStatusBar()  # 状态显示栏
        # 显示当前功能
        self.window_label = QLabel()
        # 设置显示格式
        self.window_label.setFont(QFont("SimHei", 10))
        pe = QPalette()
        pe.setColor(QPalette.WindowText, Qt.gray)
        self.window_label.setPalette(pe)
        self.process_label = QLabel("当前进度:")
        self.process_bar = QProgressBar()  # 进度条
        self.process_bar.setMaximumWidth(150)
        self.process_bar.setMaximumHeight(15)
        self.status_bar.addPermanentWidget(self.process_label)
        self.status_bar.addPermanentWidget(self.process_bar)
        self.status_bar.addPermanentWidget(self.window_label)
        self.setStatusBar(self.status_bar)  # 状态栏加入主窗口
        """定义主窗口大小"""
        self.resize(1050, 680)

    def status_show_message(self, message):
        """状态栏显示子窗口相关信息的槽函数"""
        self.status_bar.showMessage(message)

    def process_show(self, pro_list):
        """状态栏进度条显示槽函数"""
        self.process_label.setText(pro_list[0])
        self.process_bar.setRange(0, pro_list[2])
        self.process_bar.setValue(pro_list[1])

    def price_holding(self):
        """净持率-价格功能窗口"""
        self.window_label.setText("价格-净持率")
        self.net_holding_window.message_signal.connect(self.status_show_message)  # 子窗口信号绑定槽函数
        # 初始化数据
        self.net_holding_window.fill_init_data()
        self.net_holding_window.process_signal.connect(self.process_show)
        self.window_stack.setCurrentWidget(self.net_holding_window)

    def variety_percent(self):
        """品种价格指数窗口"""
        self.window_label.setText("品种权重指数")
        self.variety_price_window.message_signal.connect(self.status_show_message)
        self.variety_price_window.process_signal.connect(self.process_show)
        # 初始化
        self.variety_price_window.fill_init_data()
        self.window_stack.setCurrentWidget(self.variety_price_window)

    def main_contract(self):
        """主力合约价格指数窗口"""
        self.window_label.setText("主力合约指数")
        self.main_contract_window.message_signal.connect(self.status_show_message)
        self.main_contract_window.process_signal.connect(self.process_show)
        self.main_contract_window.fill_init_data()
        self.window_stack.setCurrentWidget(self.main_contract_window)

    def one_key_update(self):
        """一键更新菜单被点击了"""
        self.sys_window.process_signal.connect(self.process_show)
        self.sys_window.message_signal.connect(self.status_show_message)
        self.sys_window.one_key_update()

    def point_key_update(self):
        """指定更新菜单被点击了"""
        self.sys_window.point_update()
        self.sys_window.process_signal.connect(self.process_show)
        self.sys_window.message_signal.connect(self.status_show_message)

    def add_variety(self):
        """添加品种菜单被点击了"""
        self.sys_window.add_product()
        self.sys_window.process_signal.connect(self.process_show)
        self.sys_window.message_signal.connect(self.status_show_message)

    def view_lasted(self):
        """查看当前系统最新数据"""
        self.sys_window.view_lasted()
        self.sys_window.message_signal.connect(self.status_show_message)
        self.sys_window.process_signal.connect(self.process_show)

    def update_price_index(self):
        """更新品种权重价格数据库"""
        self.sys_window.update_price_index()
        self.sys_window.message_signal.connect(self.status_show_message)
        self.sys_window.process_signal.connect(self.process_show)

    def export_excel(self):
        """导出当前窗口表格数据到Excel"""
        # 获取当前窗口
        current_window = self.window_stack.currentWidget()
        if not isinstance(current_window, (NetHoldingWindow, VarietyPriceWindow, MainContractWindow)):
            return  # 非操作窗口返回
        # 保存位置选择,默认桌面
        desktop_path = self.get_desktop_path()
        save_path = QFileDialog.getExistingDirectory(self, "选择保存的位置", desktop_path)
        # print(save_path, type(save_path))
        # 实例化一个Workbook()对象(即excel文件)
        excel_book = xlwt.Workbook()
        # 新建一个名为Sheet1的excel sheet。此处的cell_overwrite_ok =True是为了能对同一个单元格重复操作。
        excel_sheet = excel_book.add_sheet('Sheet1', cell_overwrite_ok=True)
        # 获取当前窗口的表格对象和图像对象
        current_table = current_window.table_widget
        # 获取表格的行数和列数 col-列，row-行
        col_count = current_table.columnCount()
        row_count = current_table.rowCount()
        # 获取表格的头
        for col in range(col_count):
            # print(current_table.horizontalHeaderItem(col).text(), ' ', end='')
            excel_sheet.write(0, col, current_table.horizontalHeaderItem(col).text())  # 写入表头
        # print('')
        # 遍历获取数据,写入到excel
        for row in range(row_count):
            for col in range(col_count):
                # print(current_table.item(row_count-1, col).text(), ' ', end='')
                # 数据格式处理
                number = float(current_table.item(row_count-1, col).text())
                excel_sheet.write(row+1, col, number)  # 逐行写入数据
            row_count -= 1
            # print('')
        # 获取信息用来命名文件
        exchange = current_window.exchange_lib.currentText()
        variety = current_window.variety_lib.currentText()
        cur_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
        if isinstance(current_window, NetHoldingWindow):
            contract = current_window.contract_lib.currentText()
            excel_name = exchange + variety + contract + "价格净持率" + cur_time
        elif isinstance(current_window, MainContractWindow):
            excel_name = exchange + variety + "主力合约价格指数" + cur_time
        elif isinstance(current_window, VarietyPriceWindow):
            excel_name = exchange + variety + "权重价格指数" + cur_time
        else:
            return
        file_path = save_path + "/" + excel_name + ".xls"
        excel_book.save(file_path)
        # 自定义对话框询问是否打开文件
        open_dialog = QMessageBox.question(self, "成功", "导出保存成功！\n是否现在打开？", QMessageBox.Yes | QMessageBox.No)
        if open_dialog == QMessageBox.Yes:
            self.open_excel(file_path)  # 调用Microsoft Excel 打开文件

    @staticmethod
    def open_excel(path):
        """调用Microsoft Excel打开"""
        xl_app = win32com.client.Dispatch("Excel.Application")
        xl_app.Visible = True  # 是否显示Excel文件
        xl_app.Workbooks.Open(path)  # 参数:文件path

    @staticmethod
    def get_desktop_path():
        """获取用户桌面路径"""
        ilist = shell.SHGetSpecialFolderLocation(0, shellcon.CSIDL_DESKTOP)
        return shell.SHGetPathFromIDList(ilist).decode("utf-8")
