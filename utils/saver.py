# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
import win32com
from win32com.shell import shell, shellcon


def get_desktop_path():
    """获取用户桌面路径"""
    ilist = shell.SHGetSpecialFolderLocation(0, shellcon.CSIDL_DESKTOP)
    return shell.SHGetPathFromIDList(ilist).decode("utf-8")


def open_excel(path):
    """调用Microsoft Excel打开"""
    xl_app = win32com.client.Dispatch("Excel.Application")
    xl_app.Visible = True  # 是否显示Excel文件
    xl_app.Workbooks.Open(path)  # 参数:文件path