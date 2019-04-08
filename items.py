# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle

from sqlalchemy import Column, Integer, String, FLOAT
from sqlalchemy.ext.declarative import declarative_base


class Item(object):
    pass


class SHFEMsgItem(declarative_base()):
    """日交易快讯数据类"""
    __tablename__ = 'shfe_msg'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    presettlement_price = Column(Integer)
    open_price = Column(Integer)  # 开盘价
    highest_price = Column(Integer)  # 最高价
    lowest_price = Column(Integer)  # 最低价
    close_price = Column(Integer)  # 收盘价
    settlement_price = Column(Integer)  # 结算参考价
    zd1_chg = Column(Integer)  # 涨跌1
    zd2_chg = Column(Integer)  # 涨跌2
    volume = Column(Integer)  # 成交手
    open_interest = Column(Integer)  # 持仓手
    open_interest_chg = Column(Integer)  # 变化


class SHFETotalItem(declarative_base()):
    """日交易快讯总计数据模型"""
    __tablename__ = 'shfe_total'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    highest_price = Column(Integer)  # 最高价
    lowest_price = Column(Integer)  # 最低价
    avg_price = Column(FLOAT)  # 加权平均价
    volume = Column(Integer)  # 成交手
    turnover = Column(FLOAT)  # 成交额(亿元)
    year_volume = Column(FLOAT)  # 年成交手(万手)
    year_turnover = Column(FLOAT)  # 年成交额(亿元)


class SHFEMetalItem(declarative_base()):
    """有色金属指数数据模型"""
    __tablename__ = 'shfe_metal'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    last_price = Column(FLOAT)
    open_price = Column(FLOAT)
    highest_price = Column(FLOAT)
    lowest_price = Column(FLOAT)
    avg_price = Column(FLOAT)
    close_price = Column(FLOAT)
    pre_close_price = Column(FLOAT)
    up_down = Column(FLOAT)
    zd1 = Column(FLOAT)
    zd2 = Column(FLOAT)
    settlement_price = Column(FLOAT)


class SHFERkgItem(declarative_base()):
    """日排行数据类"""
    __tablename__ = 'shfe_rkg'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    rank = Column(Integer)  # 名次
    company1 = Column(String)  # 期货公司会员名称1
    company1_id = Column(String)  # 期货公司会员id1
    cj1 = Column(Integer)  # 成交量
    cj1_chg = Column(Integer)  # 成交量变化
    company2 = Column(String)  # 期货公司会员名称2
    company2_id = Column(String)  # 期货公司会员id2
    cj2 = Column(Integer)  # 持买单量
    cj2_chg = Column(Integer)  # 买单量变化
    company3 = Column(String)  # 期货公司会员名称3
    company3_id = Column(String)  # 期货公司会员id3
    cj3 = Column(Integer)  # 卖单量
    cj3_chg = Column(Integer)  # 卖单量变化


class SHFETargetItem(declarative_base()):
    """最终数据类"""
    # 显示声明关联的数据表名称
    __tablename__ = 'shfe_target'
    # 表的结构
    # 主键Id
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    price = Column(Integer)
    holdings = Column(Integer)
    b_volume = Column(Integer)
    s_volume = Column(Integer)
    net_holding = Column(Integer)
    holding_rate = Column(String)


class DCEMsgItem(declarative_base()):
    """日行情数据"""
    __tablename__ = 'dce_msg'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    open_price = Column(String)
    highest_price = Column(String)
    lowest_price = Column(String)
    close_price = Column(String)
    presettlement_price = Column(String)
    settlement_price = Column(String)
    zd = Column(String)
    zd1 = Column(String)
    volume = Column(String)
    holdings = Column(String)
    holdings_chg = Column(String)
    volume_price = Column(String)


class DCERkgItem(declarative_base()):
    __tablename__ = 'dce_rkg'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    rank = Column(String)  # 名次
    company1 = Column(String)  # 期货公司会员名称1
    cj1 = Column(String)  # 成交量
    cj1_chg = Column(String)  # 成交量变化
    company2 = Column(String)  # 期货公司会员名称2
    cj2 = Column(String)  # 持买单量
    cj2_chg = Column(String)  # 买单量变化
    company3 = Column(String)  # 期货公司会员名称3
    cj3 = Column(String)  # 卖单量
    cj3_chg = Column(String)  # 卖单量变化


class DCETargetItem(declarative_base()):
    __tablename__ = 'dce_target'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    price = Column(String)
    holdings = Column(Integer)
    b_volume = Column(Integer)
    s_volume = Column(Integer)
    net_holding = Column(Integer)
    holding_rate = Column(String)


class CFFEXMsgItem(declarative_base()):
    __tablename__ = 'cffex_msg'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    open_price = Column(String)
    highest_price = Column(String)
    lowest_price = Column(String)
    volume = Column(String)
    turnover = Column(String)
    holdings = Column(String)
    close_price = Column(String)
    settlement_price = Column(String)
    zd1 = Column(String)
    zd2 = Column(String)


class CFFEXRkgItem(declarative_base()):
    __tablename__ = 'cffex_rkg'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    data_type = Column(String)
    rank = Column(Integer)
    company = Column(String)
    company_id = Column(String)
    volume = Column(Integer)
    volume_chg = Column(Integer)


class CFFEXTargetItem(declarative_base()):
    __tablename__ = 'cffex_target'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    price = Column(Integer)
    holdings = Column(Integer)
    b_volume = Column(Integer)
    s_volume = Column(Integer)
    net_holding = Column(Integer)
    holding_rate = Column(String)


class CZCEMsgItem(declarative_base()):
    __tablename__ = 'czce_msg'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    pre_settlement = Column(String)
    open_price = Column(String)
    highest_price = Column(String)
    lowest_price = Column(String)
    close_price = Column(String)
    settlement = Column(String)
    zd1 = Column(String)
    zd2 = Column(String)
    volume = Column(String)
    open_interest = Column(String)
    decrease = Column(String)
    turnover = Column(String)
    settlement_price = Column(String)


class CZCERkgItem(declarative_base()):
    __tablename__ = 'czce_rkg'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    rank = Column(String)
    company1 = Column(String)
    cj1 = Column(String)
    cj1_chg = Column(String)
    company2 = Column(String)
    cj2 = Column(Integer)
    cj2_chg = Column(String)
    company3 = Column(String)
    cj3 = Column(Integer)
    cj3_chg = Column(String)


class CZCETargetItem(declarative_base()):
    __tablename__ = 'czce_target'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)
    name = Column(String)
    contract = Column(String)
    price = Column(String)
    holdings = Column(Integer)
    b_volume = Column(Integer)
    s_volume = Column(Integer)
    net_holding = Column(Integer)
    holding_rate = Column(String)


class SHFEPrice(declarative_base()):
    """上期所价格指数表"""
    __tablename__ = "shfe_price"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)  # 日期
    name = Column(String)  # 品种
    volume = Column(Integer)  # 总成交量
    holdings = Column(Integer)  # 总持仓量
    variety_price = Column(FLOAT)  # 价格
    contract_price = Column(FLOAT)  # 价格


class DCEPrice(declarative_base()):
    """上期所品种权重价格表"""
    __tablename__ = "dce_price"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)  # 日期
    name = Column(String)  # 品种
    volume = Column(Integer)  # 总成交量
    holdings = Column(Integer)  # 总持仓量
    variety_price = Column(FLOAT)  # 价格
    contract_price = Column(FLOAT)  # 价格


class CFFEXPrice(declarative_base()):
    """上期所品种权重价格表"""
    __tablename__ = "cffex_price"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)  # 日期
    name = Column(String)  # 品种
    volume = Column(Integer)  # 总成交量
    holdings = Column(Integer)  # 总持仓量
    variety_price = Column(FLOAT)  # 价格
    contract_price = Column(FLOAT)  # 价格


class CZCEPrice(declarative_base()):
    """上期所品种权重价格表"""
    __tablename__ = "czce_price"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String)  # 日期
    name = Column(String)  # 品种
    volume = Column(Integer)  # 总成交量
    holdings = Column(Integer)  # 总持仓量
    variety_price = Column(FLOAT)  # 价格
    contract_price = Column(FLOAT)  # 价格



