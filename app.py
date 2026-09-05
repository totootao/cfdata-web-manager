#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CFData 优选 Web 管理平台（纯 Python 标准库实现，无第三方依赖）

功能:
  1. Web 界面管理多个 API 源地址 (-nsbsourceurl)，支持增删/启用禁用
  2. 支持 cron 定时任务 + 手动触发
  3. 顺序测试所有 API 源 -> 合并去重 -> 按速度排序 -> 提取前 N 个节点
  4. 保存两种格式: TXT (ip:port#速度-数据中心-位置) + Clash YAML (trojan 节点)

用法:
  python3 app.py [--host 0.0.0.0] [--port 8088]
"""

import argparse
import base64
import csv
import hmac
import io
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import urllib.request

APP_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(APP_DIR, 'results')
WEB_DIR = os.path.join(APP_DIR, 'web')
RUNS_INDEX_PATH = os.path.join(RESULTS_DIR, 'runs.json')

# 数据目录(存放可持久化状态): Docker 环境使用挂载点 /app/data, 本地运行回退到应用目录
# 可用环境变量 CFDATA_DATA_DIR 覆盖
_env_data = os.environ.get('CFDATA_DATA_DIR')
if _env_data:
    DATA_DIR = _env_data
elif os.path.isdir('/app/data'):
    DATA_DIR = '/app/data'
else:
    DATA_DIR = APP_DIR
CONFIG_PATH = os.path.join(DATA_DIR, 'config.json')
CFDATA_CONFIG_PATH = os.path.join(DATA_DIR, 'cfdata-config.json')
LOCATIONS_CACHE_PATH = os.path.join(DATA_DIR, 'locations.json')
# GeoLite2-ASN.mmdb 共享缓存: CLI 在工作目录找不到该文件时会重新下载,
# 预先从数据目录拷贝可让每个任务目录都复用同一份, 避免重复下载
MMDB_NAME = 'GeoLite2-ASN.mmdb'
MMDB_CACHE_PATH = os.path.join(DATA_DIR, MMDB_NAME)
# 历史节点池(跨轮复测的达标节点集合), 同样存放于数据目录以持久化
HISTORY_POOL_PATH = os.path.join(DATA_DIR, 'history_nodes.json')
HISTORY_SOURCE_ID = '__history__'
HISTORY_SOURCE_NAME = '历史节点'
# 固定的"最新结果"目录: 每次任务成功后同步覆盖, 路径不变便于外部订阅
LATEST_DIR = os.path.join(RESULTS_DIR, 'latest')
# IPv4 与 IPv6 结果分开输出: 主文件(top_nodes.*)只含 IPv4,
# IPv6(官方优选等)单独输出 top_nodes_v6.* / all_sorted_v6.txt
LATEST_FILES = ('top_nodes.txt', 'top_nodes.yaml', 'all_sorted.txt',
                'top_nodes_v6.txt', 'top_nodes_v6.yaml', 'all_sorted_v6.txt',
                'top_by_source.txt', 'top_by_source.yaml')
# 质检记录(对 latest Top 节点的复测剔除结果), 存放于数据目录以持久化
QA_RUNS_PATH = os.path.join(DATA_DIR, 'qa_runs.json')
# 质检固定工作目录: 只保留最近一次质检的日志/输入导出/实测结果(每次覆盖)
QA_WORK_DIR = os.path.join(RESULTS_DIR, 'qa')
# 质检输入基准: 完整任务提取的原始 Top 节点列表, 每次质检都全量复测这批节点,
# 不随剔除缩小 —— 被剔除的节点速度恢复后自动回归订阅文件
QA_INPUT_NAME = 'qa_input.txt'
QA_INPUT_PATH = os.path.join(LATEST_DIR, QA_INPUT_NAME)
# 订阅模板(FlClash 完整订阅转换): 缓存于数据目录, 每天最多重新拉取一次
SUB_TEMPLATE_PATH = os.path.join(DATA_DIR, 'sub_template.yaml')
SUB_TEMPLATE_META_PATH = os.path.join(DATA_DIR, 'sub_template.json')
SUB_TEMPLATE_STALE_HOURS = 24
# 任务结束时打印到日志的结果清单, 每个栈最多打印的行数(超出截断并提示见文件,
# 避免 top_n 设置很大时把日志缓冲挤满)
SUMMARY_MAX_LINES = 50
# 质检结果汇总里"剔除清单"最多打印的行数(逐行日志已有完整明细, 这里只作一眼可览的摘要)
QA_PRUNE_MAX_LINES = 10

# 子进程退出等待兜底: 超时或取消时先发 SIGTERM, 等待该秒数仍未退出则 SIGKILL。
# 若 cfdata 忽略 SIGTERM(网络 IO 卡死等), 无兜底的 wait() 会永久阻塞, 任务线程
# 卡在 _exec_cmd 内部 -> _running 恒为 True -> 之后所有任务与质检都被"正在运行
# 中"拒绝, 只能重启服务才能恢复。
PROC_TERM_WAIT = 30
PROC_KILL_WAIT = 10

# ---------------------------------------------------------------- 可选访问控制
# 服务默认无任何鉴权, 监听 0.0.0.0 时任何能访问端口的人都能改配置/删源/下载订阅。
# 同时设置 CFDATA_AUTH_USER 与 CFDATA_AUTH_PASS 即启用全站 HTTP Basic 认证
# (不设置时行为与旧版本完全一致, 不影响已有部署)。
# 订阅客户端(Clash 等)不便填密码时, 可用 CFDATA_SUB_TOKEN 给 /api/download/**
# 配一个可写进 URL 的令牌: /api/download/latest/top_nodes.yaml?token=xxx
AUTH_USER = (os.environ.get('CFDATA_AUTH_USER') or '').strip()
AUTH_PASS = os.environ.get('CFDATA_AUTH_PASS') or ''
AUTH_ENABLED = bool(AUTH_USER and AUTH_PASS)
SUB_TOKEN = (os.environ.get('CFDATA_SUB_TOKEN') or '').strip()

# CSV 导出字段(传给 cfdata -fields), 与中文表头一一对应
CSV_FIELDS = 'ipport,latency,speed,dc,loc,region,city'
HEADER_IPPORT = 'ip:port'
HEADER_LATENCY = '网络延迟'
HEADER_SPEED = '下载速度'
HEADER_DC = '数据中心'
HEADER_LOC = '源IP位置'
HEADER_REGION = '地区'
HEADER_CITY = '城市'

# ---------------------------------------------------------------- 节点名 中英文对照
# cfdata 输出的 region 为 ISO-3166-1 alpha-2 国家/地区码, dc 为机场码(大写),
# loc/city 多为英文地名(如 Hong Kong), 但也可能直接是两位国家码(如 HK)。
# 节点名语言设为中文时, 按以下映射把对应字段替换为中文, 未知值原样保留
# (避免误改用户自定义地名)。
REGION_ZH = {
    'HK': '香港', 'TW': '台湾', 'MO': '澳门', 'CN': '中国',
    'US': '美国', 'JP': '日本', 'SG': '新加坡', 'KR': '韩国', 'KP': '朝鲜',
    'GB': '英国', 'DE': '德国', 'FR': '法国', 'NL': '荷兰', 'ES': '西班牙',
    'IT': '意大利', 'CA': '加拿大', 'AU': '澳大利亚', 'NZ': '新西兰',
    'IN': '印度', 'BR': '巴西', 'MX': '墨西哥', 'RU': '俄罗斯', 'TR': '土耳其',
    'SE': '瑞典', 'NO': '挪威', 'DK': '丹麦', 'FI': '芬兰', 'PL': '波兰',
    'CH': '瑞士', 'AT': '奥地利', 'BE': '比利时', 'IE': '爱尔兰', 'PT': '葡萄牙',
    'CZ': '捷克', 'RO': '罗马尼亚', 'GR': '希腊', 'HU': '匈牙利', 'UA': '乌克兰',
    'AE': '阿联酋', 'SA': '沙特', 'IL': '以色列', 'ZA': '南非', 'EG': '埃及',
    'TH': '泰国', 'VN': '越南', 'MY': '马来西亚', 'ID': '印尼', 'PH': '菲律宾',
}
DC_ZH = {
    'HKG': '香港', 'TPE': '台北', 'KHH': '高雄', 'NRT': '东京', 'HND': '东京羽田',
    'KIX': '大阪', 'NGO': '名古屋', 'FUK': '福冈', 'CTS': '札幌', 'OKA': '冲绳',
    'ICN': '首尔', 'PUS': '釜山', 'SIN': '新加坡', 'BKK': '曼谷', 'KUL': '吉隆坡',
    'MNL': '马尼拉', 'CGK': '雅加达', 'SGN': '胡志明', 'HAN': '河内', 'RGN': '仰光',
    'BOM': '孟买', 'DEL': '德里', 'MAA': '金奈', 'BLR': '班加罗尔', 'CCU': '加尔各答',
    'LAX': '洛杉矶', 'SJC': '圣何塞', 'SFO': '旧金山', 'SEA': '西雅图', 'PDX': '波特兰',
    'SNA': '圣安娜', 'PHX': '菲尼克斯', 'LAS': '拉斯维加斯', 'SAN': '圣迭戈',
    'ONT': '安大略', 'SMF': '萨克拉门托', 'OAK': '奥克兰', 'BOI': '博伊西',
    'ORD': '芝加哥', 'DFW': '达拉斯', 'IAH': '休斯顿', 'ATL': '亚特兰大', 'MIA': '迈阿密',
    'MCO': '奥兰多', 'BOS': '波士顿', 'JFK': '纽约', 'EWR': '纽瓦克', 'IAD': '华盛顿',
    'DCA': '华盛顿', 'CLT': '夏洛特', 'DET': '底特律', 'MSP': '明尼阿波利斯',
    'YYZ': '多伦多', 'YVR': '温哥华', 'YUL': '蒙特利尔', 'YEG': '埃德蒙顿',
    'MEX': '墨西哥城', 'GDL': '瓜达拉哈拉',
    'LHR': '伦敦', 'MAN': '曼彻斯特', 'EDI': '爱丁堡', 'DUB': '都柏林',
    'FRA': '法兰克福', 'BER': '柏林', 'MUC': '慕尼黑', 'DUS': '杜塞尔多夫',
    'HAM': '汉堡', 'STU': '斯图加特', 'CGN': '科隆', 'STR': '斯图加特',
    'CDG': '巴黎', 'AMS': '阿姆斯特丹', 'RTM': '鹿特丹', 'BRU': '布鲁塞尔',
    'LUX': '卢森堡', 'MAD': '马德里', 'BCN': '巴塞罗那', 'VLC': '瓦伦西亚',
    'LIS': '里斯本', 'OTP': '布加勒斯特', 'WAW': '华沙', 'PRG': '布拉格',
    'VIE': '维也纳', 'ZRH': '苏黎世', 'GVA': '日内瓦', 'MIL': '米兰', 'FCO': '罗马',
    'CPH': '哥本哈根', 'OSL': '奥斯陆', 'ARN': '斯德哥尔摩', 'GOT': '哥德堡',
    'HEL': '赫尔辛基', 'IST': '伊斯坦布尔', 'ATH': '雅典', 'BUD': '布达佩斯',
    'DXB': '迪拜', 'RUH': '利雅得', 'JED': '吉达', 'TLV': '特拉维夫',
    'JNB': '约翰内斯堡', 'CPT': '开普敦', 'LOS': '拉各斯', 'CAI': '开罗',
    'GRU': '圣保罗', 'GIG': '里约', 'EZE': '布宜诺斯艾利斯', 'SCL': '圣地亚哥',
    'BOG': '波哥大', 'LIM': '利马', 'UIO': '基多',
    'SYD': '悉尼', 'MEL': '墨尔本', 'BNE': '布里斯班', 'PER': '珀斯', 'AKL': '奥克兰',
    'PPT': '帕皮提', 'NOU': '努美阿',
}
LOC_CITY_ZH = {
    'Hong Kong': '香港', 'Taipei': '台北', 'Kaohsiung': '高雄', 'Tokyo': '东京',
    'Osaka': '大阪', 'Nagoya': '名古屋', 'Fukuoka': '福冈', 'Sapporo': '札幌',
    'Seoul': '首尔', 'Busan': '釜山', 'Singapore': '新加坡', 'Bangkok': '曼谷',
    'Kuala Lumpur': '吉隆坡', 'Manila': '马尼拉', 'Jakarta': '雅加达',
    'Ho Chi Minh City': '胡志明', 'Hanoi': '河内', 'Yangon': '仰光',
    'Mumbai': '孟买', 'Delhi': '德里', 'Chennai': '金奈', 'Bengaluru': '班加罗尔',
    'Los Angeles': '洛杉矶', 'San Jose': '圣何塞', 'San Francisco': '旧金山',
    'Seattle': '西雅图', 'Portland': '波特兰', 'Santa Ana': '圣安娜',
    'Phoenix': '菲尼克斯', 'Las Vegas': '拉斯维加斯', 'San Diego': '圣迭戈',
    'Ontario': '安大略', 'Sacramento': '萨克拉门托', 'Oakland': '奥克兰',
    'Chicago': '芝加哥', 'Dallas': '达拉斯', 'Houston': '休斯顿', 'Atlanta': '亚特兰大',
    'Miami': '迈阿密', 'Orlando': '奥兰多', 'Boston': '波士顿', 'New York': '纽约',
    'Newark': '纽瓦克', 'Washington': '华盛顿', 'Charlotte': '夏洛特',
    'Detroit': '底特律', 'Minneapolis': '明尼阿波利斯',
    'Toronto': '多伦多', 'Vancouver': '温哥华', 'Montreal': '蒙特利尔',
    'Mexico City': '墨西哥城', 'Guadalajara': '瓜达拉哈拉',
    'London': '伦敦', 'Manchester': '曼彻斯特', 'Edinburgh': '爱丁堡',
    'Dublin': '都柏林', 'Frankfurt': '法兰克福', 'Berlin': '柏林', 'Munich': '慕尼黑',
    'Hamburg': '汉堡', 'Stuttgart': '斯图加特', 'Paris': '巴黎',
    'Amsterdam': '阿姆斯特丹', 'Brussels': '布鲁塞尔', 'Luxembourg': '卢森堡',
    'Madrid': '马德里', 'Barcelona': '巴塞罗那', 'Lisbon': '里斯本',
    'Bucharest': '布加勒斯特', 'Warsaw': '华沙', 'Prague': '布拉格',
    'Vienna': '维也纳', 'Zurich': '苏黎世', 'Geneva': '日内瓦', 'Milan': '米兰',
    'Rome': '罗马', 'Copenhagen': '哥本哈根', 'Oslo': '奥斯陆',
    'Stockholm': '斯德哥尔摩', 'Helsinki': '赫尔辛基', 'Istanbul': '伊斯坦布尔',
    'Athens': '雅典', 'Budapest': '布达佩斯',
    'Dubai': '迪拜', 'Riyadh': '利雅得', 'Jeddah': '吉达', 'Tel Aviv': '特拉维夫',
    'Johannesburg': '约翰内斯堡', 'Cape Town': '开普敦', 'Lagos': '拉各斯',
    'Cairo': '开罗', 'São Paulo': '圣保罗', 'Rio de Janeiro': '里约',
    'Buenos Aires': '布宜诺斯艾利斯', 'Santiago': '圣地亚哥',
    'Sydney': '悉尼', 'Melbourne': '墨尔本', 'Brisbane': '布里斯班',
    'Perth': '珀斯', 'Auckland': '奥克兰',
}
# 国家/地区英文全称(出现在 loc/city 时)也映射到中文
REGION_NAME_ZH = {
    'United States': '美国', 'United Kingdom': '英国', 'Germany': '德国',
    'France': '法国', 'Netherlands': '荷兰', 'Spain': '西班牙', 'Italy': '意大利',
    'Canada': '加拿大', 'Australia': '澳大利亚', 'New Zealand': '新西兰',
    'India': '印度', 'Brazil': '巴西', 'Mexico': '墨西哥', 'Russia': '俄罗斯',
    'Turkey': '土耳其', 'Sweden': '瑞典', 'Norway': '挪威', 'Denmark': '丹麦',
    'Finland': '芬兰', 'Poland': '波兰', 'Switzerland': '瑞士', 'Austria': '奥地利',
    'Belgium': '比利时', 'Ireland': '爱尔兰', 'Portugal': '葡萄牙',
    'Czech Republic': '捷克', 'Romania': '罗马尼亚', 'Greece': '希腊',
    'Hungary': '匈牙利', 'Ukraine': '乌克兰', 'United Arab Emirates': '阿联酋',
    'Saudi Arabia': '沙特', 'Israel': '以色列', 'South Africa': '南非',
    'Thailand': '泰国', 'Vietnam': '越南', 'Malaysia': '马来西亚',
    'Indonesia': '印尼', 'Philippines': '菲律宾', 'Japan': '日本',
    'South Korea': '韩国', 'Singapore': '新加坡', 'Hong Kong': '香港',
}


def localize_node_fields(fields):
    """把节点字段里可识别的地区/数据中心英文替换为中文。

    仅替换 REGION_ZH/DC_ZH/LOC_CITY_ZH/REGION_NAME_ZH 命中项, 未知值原样保留,
    绝不猜测或误改用户自定义地名。返回新字典, 不改动原 fields。
    """
    out = dict(fields)
    region = (out.get('region') or '').strip().upper()
    if region in REGION_ZH:
        out['region'] = REGION_ZH[region]
    dc = (out.get('dc') or '').strip().upper()
    if dc in DC_ZH:
        out['dc'] = DC_ZH[dc]
    for k in ('loc', 'city'):
        raw = (out.get(k) or '').strip()
        if not raw:
            continue
        if raw in LOC_CITY_ZH:
            out[k] = LOC_CITY_ZH[raw]
        elif raw.title() in REGION_NAME_ZH:
            out[k] = REGION_NAME_ZH[raw.title()]
        elif raw.upper() in REGION_ZH:
            # 源IP位置/城市也可能是两位国家码(如 HK/TW/SG), 同样转中文
            out[k] = REGION_ZH[raw.upper()]
    return out


DEFAULT_CONFIG = {
    'sources': [
        {
            'id': 'src_hk_demo',
            'name': 'HK 优选',
            'url': 'https://bestcf.pages.dev/random-region/HK/all.txt',
            'enabled': True,
        }
    ],
    'cron': {'enabled': False, 'expr': '0 8 * * *'},
    'settings': {
        'binary_path': '',          # 留空则自动检测
        'top_n': 20,                # 提取前 N 个节点(全局合并排序后)
        'per_source_top_n': 5,      # 每个源单独提取速度最快的前 N 个节点(输出 top_by_source.txt)
        # 稳定节点(latest/top_stable.*): 入选需同时满足 ①本次全量任务复测达标
        # ②累计出现次数严格大于该值(默认 10, 即至少 11 次); 按出现次数降序取前 top_n
        'stable_min_hits': 10,
        'source_retries': 3,        # 源执行失败(退出码非 0/异常)自动重试次数(0 = 不重试; 测速未达标不重试)
        'source_retry_delay': 5,    # 重试间隔秒数
        # ---- 历史节点复测 ----
        'history_test_enabled': True,   # 把池内历史达标节点作为第一个"源"参与每轮测试
        'history_pool_capacity': 250,   # 池容量上限, 超出挤出最近速度最慢的
        'history_window_runs': 5,       # 滚动窗口: 只保留最近 N 次运行出现过的节点
        'history_evict_fails': 3,       # 连续 K 次复测不达标即移出池
        # ---- 官方 IPv6 优选 ----
        'official_v6_enabled': False,   # 作为附加伪源扫描 Cloudflare 官方 IPv6 地址库(-mode=official -offiptype=6)
        'official_v6_count': 20,        # -offspeedlimit 官方模式测速达标结果上限(达标即停止)
        'official_v6_delay': 500,       # -offdelay 官方模式延迟阈值(毫秒), 超过剔除
        # ---- FlClash 订阅模板转换 ----
        'sub_convert_enabled': False,   # 把 latest 的 top_nodes.yaml / top_nodes_v6.yaml 按模板转为完整 Clash 订阅
        'sub_convert_url': '',          # 订阅模板链接(完整 Clash 配置; 其节点列表将被平台实测 Top 节点替换, 规则/策略组保留)
        'speedtest_threads': 5,     # -nsbspeedtest 测速线程数
        'speed_min': 5.0,           # -nsbspeedmin 最低速度 MB/s
        'speed_limit': 9999,        # -nsbspeedlimit 测速结果上限
        'qualified': True,          # -nsbqualified
        'result_limit': 1000,       # -nsbresultlimit
        'threads': 100,             # -threads 扫描并发
        'ip_type': 'all',           # -nsbiptype
        'tls': True,                # -nsbtls
        'skip_geo_check': True,     # -skipgeo 跳过代理/地区环境验证（服务器/Docker 无交互环境必须开启）
        'timeout_minutes': 0,       # 单个源超时(分钟), 0 = 不限制
        'node': {
            'name_template': '{dc}-{loc}-{speed}MB/s',
            'name_lang': 'zh',        # zh=节点名使用中文(地区/数据中心转中文) / en=保留英文原始字段
            'password': '4c89536c-905b-4c77-8ea3-3350a6060c68',
            'sni': 'edgetunnel-ekw.pages.dev',
            'host': 'edgetunnel-ekw.pages.dev',
            'path': '/',
            'client_fingerprint': 'chrome',
            'skip_cert_verify': False,
        },
    },
}


# ---------------------------------------------------------------- 工具函数
def now_str(fmt='%Y-%m-%d %H:%M:%S'):
    return datetime.now().strftime(fmt)


def new_id(prefix):
    return '%s_%s' % (prefix, uuid.uuid4().hex[:10])


def migrate_legacy_state():
    """把旧位置(应用目录)的持久化文件迁移到数据目录; 仅在数据目录缺少对应文件时执行"""
    if os.path.realpath(DATA_DIR) == os.path.realpath(APP_DIR):
        return []
    moved = []
    for name, dst in (('config.json', CONFIG_PATH),
                      ('cfdata-config.json', CFDATA_CONFIG_PATH),
                      ('locations.json', LOCATIONS_CACHE_PATH)):
        src = os.path.join(APP_DIR, name)
        if os.path.exists(src) and not os.path.exists(dst):
            try:
                os.makedirs(DATA_DIR, exist_ok=True)
                shutil.copyfile(src, dst)
                moved.append(name)
            except Exception:
                pass
    return moved


def deep_merge(base, override):
    """递归合并字典, override 覆盖 base"""
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def parse_speed_mb(value):
    """'42.41MB/s' -> 42.41 ; 失败返回 None"""
    value = (value or '').strip()
    if 'MB/s' not in value:
        return None
    try:
        return float(value.replace('MB/s', '').strip())
    except ValueError:
        return None


def parse_latency_ms(value):
    """'45.2ms' -> 45.2"""
    try:
        return float((value or '').strip().replace('ms', '').strip())
    except ValueError:
        return float('inf')


def bracket_v6_ipport(ipport):
    """官方扫描 CSV 的 ipport 为裸 IPv6(如 2606:4700::1:443), 写入测速输入文件前
    规范化为方括号格式 [2606:4700::1]:443 —— cfdata 的 -nsbfile 输入解析对带
    方括号的 IPv6 支持最完整(与平台质检/历史池导出的输入格式一致)。IPv4 原样返回。

    仅当确实形如"IPv6 + 端口"时才改写: rpartition 对**不带端口**的裸地址会把末段
    误当端口(如 2606:4700::1 -> [2606:4700:]:1), 因此改写前校验端口取值范围与
    IP 地址合法性; 任一项不通过则原样返回, 避免产出错误的测速输入。
    """
    if not ipport or ipport.startswith('[') or ipport.count(':') < 2:
        return ipport
    ip, sep, port = ipport.rpartition(':')
    if not sep or not ip or ip.endswith(':') or not port.isdigit():
        return ipport
    if not (0 < int(port) < 65536):
        return ipport
    try:
        socket.inet_pton(socket.AF_INET6, ip)
    except (OSError, ValueError):
        return ipport
    return '[%s]:%s' % (ip, port)


def yaml_value(v):
    """按附件风格输出 YAML 标量: 简单值不加引号, 含特殊字符时加双引号"""
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    if s == '':
        return '""'
    if re.fullmatch(r'[A-Za-z0-9_.:/@+-]+', s):
        return s
    return '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'


def render_name_template(template, fields):
    """安全渲染节点名模板, 未知占位符替换为空串"""
    def repl(m):
        key = m.group(1)
        val = fields.get(key, '')
        return str(val) if val is not None else ''
    return re.sub(r'\{(\w+)\}', repl, template or '')


# ---------------------------------------------------------------- 订阅模板(FlClash 完整订阅转换)
_SUB_TPL_LOCK = threading.RLock()


def _sub_template_meta():
    """读取订阅模板缓存元数据; 无缓存返回 {}"""
    try:
        with open(SUB_TEMPLATE_META_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _sub_template_age_hours():
    """模板缓存年龄(小时); 从未拉取返回 None"""
    fetched = _sub_template_meta().get('fetched_at') or ''
    try:
        t = datetime.strptime(fetched, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None
    return (datetime.now() - t).total_seconds() / 3600.0


def _sub_template_stats(text):
    """统计模板的节点/策略组/规则数量(状态展示与日志用)"""
    proxies = groups = rules = 0
    section = ''
    for ln in text.splitlines():
        if ln == 'proxies:':
            section = 'p'
        elif ln == 'proxy-groups:':
            section = 'g'
        elif ln == 'rules:':
            section = 'r'
        elif section == 'p' and re.match(r'\s*-\s*\{', ln):
            proxies += 1
        elif section == 'g' and re.match(r'\s*-\s*name:', ln):
            groups += 1
        elif section == 'r' and ln.strip():
            rules += 1
    return proxies, groups, rules


def fetch_sub_template(url, timeout=30):
    """拉取订阅模板文本; 支持 http(s):// 与 file://(离线/测试); 校验为 Clash 配置"""
    url = (url or '').strip()
    if not url:
        raise ValueError('订阅模板链接为空')
    if url.startswith('file://'):
        with open(url[len('file://'):], 'r', encoding='utf-8') as f:
            return f.read()
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        raise ValueError('订阅模板链接必须是 http(s):// 或 file:// 链接')
    req = urllib.request.Request(url, headers={'User-Agent': 'clash.meta/1.18.0'})
    last = None
    for _ in range(3):  # 转换后端不稳定, 重试 3 次
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                text = resp.read().decode('utf-8', 'replace')
            if 'proxies:' not in text or 'proxy-groups:' not in text:
                raise ValueError('返回内容不是有效的 Clash 配置(缺少 proxies/proxy-groups)')
            return text
        except Exception as e:
            last = e
            time.sleep(2)
    raise RuntimeError('获取订阅模板失败(已重试 3 次): %s' % last)


def refresh_sub_template(settings, force=False, ignore_enabled=False):
    """按需刷新订阅模板缓存(超过 24 小时或强制); 返回 (模板文本|None, 状态消息)

    拉取失败时沿用上次缓存(仅记录消息不中断任务); 未启用且非强制时返回 (None, 提示)。
    """
    url = (settings.get('sub_convert_url') or '').strip()
    if not url:
        return None, '订阅模板链接为空, 请先填写模板订阅链接'
    if not settings.get('sub_convert_enabled') and not ignore_enabled:
        return None, '订阅模板转换未启用'
    with _SUB_TPL_LOCK:
        cached = None
        if os.path.isfile(SUB_TEMPLATE_PATH):
            try:
                with open(SUB_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
                    cached = f.read()
            except Exception:
                cached = None
        age = _sub_template_age_hours()
        if not force and cached is not None and age is not None \
                and age < SUB_TEMPLATE_STALE_HOURS:
            return cached, ('订阅模板: 沿用缓存(%.1f 小时前拉取, 未满 %d 小时不重复获取)'
                            % (age, SUB_TEMPLATE_STALE_HOURS))
        try:
            text = fetch_sub_template(url)
        except Exception as e:
            if cached is not None:
                return cached, '订阅模板: 本次获取失败(%s), 沿用上次缓存' % e
            return None, '订阅模板: 获取失败且无缓存(%s)' % e
        proxies, groups, rules = _sub_template_stats(text)
        meta = {'fetched_at': now_str(), 'url': url,
                'proxy_count': proxies, 'group_count': groups, 'rule_count': rules}
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = SUB_TEMPLATE_PATH + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write(text)
        os.replace(tmp, SUB_TEMPLATE_PATH)
        tmp = SUB_TEMPLATE_META_PATH + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        os.replace(tmp, SUB_TEMPLATE_META_PATH)
        return text, ('订阅模板: 已刷新(%d 个节点 / %d 个策略组 / %d 条规则, 缓存于数据目录)'
                      % (proxies, groups, rules))


def load_sub_template_text():
    """读取缓存的订阅模板文本; 无缓存返回 None"""
    try:
        with open(SUB_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception:
        return None


def sub_template_status():
    """订阅模板状态(设置页展示): 启用情况/缓存时间/模板规模"""
    cfg = STORE.get()
    settings = cfg.get('settings', {})
    meta = _sub_template_meta()
    age = _sub_template_age_hours()
    return {
        'enabled': bool(settings.get('sub_convert_enabled')),
        'url': (settings.get('sub_convert_url') or '').strip(),
        'exists': os.path.isfile(SUB_TEMPLATE_PATH),
        'fetched_at': meta.get('fetched_at') or '',
        'age_hours': None if age is None else round(age, 1),
        'proxy_count': meta.get('proxy_count'),
        'group_count': meta.get('group_count'),
        'rule_count': meta.get('rule_count'),
        'stale_hours': SUB_TEMPLATE_STALE_HOURS,
    }


def _flow_scalar(v):
    """YAML 流式映射({k: v})里的标量: 简单值不加引号, 其余加双引号(IPv6 地址含冒号)"""
    s = str(v)
    if re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._/@+-]*', s):
        return s
    return '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'


def render_clash_subscription(template_text, entries):
    """按模板生成完整 Clash 订阅文本; entries: [(节点名, server, port)]

    保留模板的 DNS/端口/策略组结构与全部规则原文; proxies 段替换为平台实测 Top
    节点(连接参数 uuid/servername/ws-opts 克隆自模板首个节点, 只换 name/server/
    port —— 与原订阅"同 uuid 不同 IP"机制一致); 各策略组成员列表中的模板节点名
    同步替换为我们的节点名(DIRECT/REJECT/其他组引用成员原样保留)。
    模板结构不符合预期时抛 ValueError, 由调用方回退片段格式。
    """
    if not entries:
        raise ValueError('节点列表为空')
    lines = template_text.splitlines()
    idx_proxies = idx_groups = idx_rules = None
    for i, ln in enumerate(lines):
        if ln == 'proxies:' and idx_proxies is None:
            idx_proxies = i
        elif ln == 'proxy-groups:' and idx_groups is None:
            idx_groups = i
        elif ln == 'rules:' and idx_rules is None:
            idx_rules = i
    if idx_proxies is None or idx_groups is None or idx_groups <= idx_proxies:
        raise ValueError('模板缺少 proxies/proxy-groups 顶层段')
    groups_end = idx_rules if (idx_rules is not None and idx_rules > idx_groups) \
        else len(lines)

    # ---- proxies 段: 模板节点行(流式映射)与首个节点的参数模板 ----
    proxy_line_re = re.compile(r'^\s*-\s*\{.*\}\s*$')
    proxy_lines = [ln for ln in lines[idx_proxies + 1:idx_groups]
                   if proxy_line_re.match(ln)]
    if not proxy_lines:
        raise ValueError('模板 proxies 段没有流式节点行')
    head_re = re.compile(r'\bname:\s*([^,{}]+?),\s*\bserver:\s*([^,{}]+?),\s*\bport:\s*([^,{}]+?),')
    tpl_names = set()
    for pl in proxy_lines:
        m = head_re.search(pl)
        if m:
            # 模板节点名常带引号({name: "节点A", ...}), 而 proxy-groups 的成员列表里
            # 写的是不带引号的名字(- 节点A)。此处去掉引号再比对, 否则成员名对不上,
            # 策略组成员不会替换 —— 生成的订阅里策略组将全部指向已不存在的旧节点。
            tpl_names.add(m.group(1).strip().strip('"\''))
    first = proxy_lines[0]
    if not head_re.search(first):
        raise ValueError('模板节点行无法解析 name/server/port')

    def _proxy_line(name, server, port):
        ln = re.sub(r'\bname:\s*[^,{}]+?,',
                    lambda mm: 'name: %s,' % _flow_scalar(name), first, count=1)
        ln = re.sub(r'\bserver:\s*[^,{}]+?,',
                    lambda mm: 'server: %s,' % _flow_scalar(server), ln, count=1)
        ln = re.sub(r'\bport:\s*[^,{}]+?,',
                    lambda mm: 'port: %s,' % str(port), ln, count=1)
        return ln

    # ---- proxy-groups 段: 成员列表里的模板节点名替换为我们的节点名 ----
    group_flow_re = re.compile(r'^\s*-\s*\{')
    group_start_re = re.compile(r'^\s*-\s*name:')
    out_groups = []
    in_members = False
    injected = False
    for ln in lines[idx_groups + 1:groups_end]:
        if group_flow_re.match(ln):
            raise ValueError('模板 proxy-groups 使用流式行, 暂不支持解析')
        if group_start_re.match(ln):
            injected = False
            in_members = False
            out_groups.append(ln)
            continue
        if re.match(r'^\s*[\w-]+\s*:', ln):  # 组内键行(type:/url:/proxies: 等)
            in_members = ln.strip().startswith('proxies:')
            out_groups.append(ln)
            continue
        m = re.match(r'^(\s*-\s+)(.+?)\s*$', ln)
        if m and in_members and m.group(2).strip() in tpl_names:
            if not injected:  # 首个节点成员处展开全部新节点, 后续模板节点行删除
                for name, _srv, _port in entries:
                    out_groups.append(m.group(1) + _flow_scalar(name))
                injected = True
            continue
        out_groups.append(ln)

    header = lines[:idx_proxies]
    new_proxies = ['proxies:'] + [_proxy_line(n, s, p) for n, s, p in entries]
    tail = lines[groups_end:]
    return '\n'.join(header + new_proxies + ['proxy-groups:'] + out_groups + tail) + '\n'


def unique_name(name, used):
    """节点名去重: 冲突时追加 #2/#3 序号"""
    base, i = name, 2
    while name in used:
        name = '%s #%d' % (base, i)
        i += 1
    used.add(name)
    return name


# ---------------------------------------------------------------- 配置管理
class ConfigStore:
    def __init__(self):
        self._lock = threading.RLock()
        self._cfg = None
        self.load()

    def load(self):
        with self._lock:
            cfg = dict(DEFAULT_CONFIG)
            if os.path.exists(CONFIG_PATH):
                try:
                    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    cfg = deep_merge(DEFAULT_CONFIG, data)
                except Exception as e:
                    sys.stderr.write('读取配置失败(%s), 使用默认配置\n' % e)
            cfg.setdefault('sources', [])
            cfg.setdefault('cron', dict(DEFAULT_CONFIG['cron']))
            cfg.setdefault('settings', dict(DEFAULT_CONFIG['settings']))
            self._cfg = cfg
            return cfg

    def save(self):
        with self._lock:
            try:
                os.makedirs(DATA_DIR, exist_ok=True)
            except Exception:
                pass
            tmp = CONFIG_PATH + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(self._cfg, f, ensure_ascii=False, indent=2)
            os.replace(tmp, CONFIG_PATH)

    def get(self):
        with self._lock:
            return json.loads(json.dumps(self._cfg))  # 深拷贝

    def update(self, patch):
        with self._lock:
            self._cfg = deep_merge(self._cfg, patch)
            self.save()
            return self.get()

    # ---- 源管理 ----
    def add_source(self, name, url, enabled=True):
        with self._lock:
            url = (url or '').strip()
            if not url:
                raise ValueError('API 地址不能为空')
            parsed = urlparse(url)
            if parsed.scheme not in ('http', 'https') or not parsed.netloc:
                raise ValueError('API 地址必须是有效的 http/https 链接')
            for s in self._cfg['sources']:
                if s['url'] == url:
                    raise ValueError('该 API 地址已存在')
            src = {'id': new_id('src'), 'name': (name or '').strip() or '未命名源',
                   'url': url, 'enabled': bool(enabled)}
            self._cfg['sources'].append(src)
            self.save()
            return src

    def update_source(self, src_id, patch):
        with self._lock:
            for s in self._cfg['sources']:
                if s['id'] == src_id:
                    if 'name' in patch:
                        s['name'] = (patch['name'] or '').strip() or s['name']
                    if 'url' in patch and (patch['url'] or '').strip():
                        u = patch['url'].strip()
                        p = urlparse(u)
                        if p.scheme not in ('http', 'https') or not p.netloc:
                            raise ValueError('API 地址必须是有效的 http/https 链接')
                        s['url'] = u
                    if 'enabled' in patch:
                        s['enabled'] = bool(patch['enabled'])
                    self.save()
                    return s
            raise KeyError('源不存在')

    def delete_source(self, src_id):
        with self._lock:
            before = len(self._cfg['sources'])
            self._cfg['sources'] = [s for s in self._cfg['sources'] if s['id'] != src_id]
            if len(self._cfg['sources']) == before:
                raise KeyError('源不存在')
            self.save()

    def enabled_sources(self):
        with self._lock:
            return [dict(s) for s in self._cfg['sources'] if s.get('enabled')]


# ---------------------------------------------------------------- 历史节点池
class HistoryPool:
    """历史达标节点池: 滚动窗口并集 + 连续失败淘汰 + 容量上限

    池文件结构与数据目录内其他状态文件一致, Docker 挂载 /app/data 即可跨容器保留。
    每个节点记录原始来源(首次发现它的 API 源), 复测只刷新速度/计数, 不改变归属。
    """

    def __init__(self, path=HISTORY_POOL_PATH):
        self._path = path
        self._lock = threading.RLock()
        self._nodes = {}        # ipport -> entry
        self._recent_runs = []  # 最近 N 次运行 ID(滚动窗口)
        self._load()

    # ---- 持久化 ----
    def _load(self):
        if not os.path.exists(self._path):
            return
        try:
            with open(self._path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            nodes = data.get('nodes') or {}
            if isinstance(nodes, dict):
                self._nodes = {k: v for k, v in nodes.items() if isinstance(v, dict) and k}
            runs = data.get('recent_runs') or []
            if isinstance(runs, list):
                self._recent_runs = [str(r) for r in runs]
        except Exception as e:
            sys.stderr.write('读取历史节点池失败(%s), 从空池开始\n' % e)

    def _save(self):
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            tmp = self._path + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump({'version': 1, 'updated_at': now_str(),
                           'recent_runs': self._recent_runs, 'nodes': self._nodes},
                          f, ensure_ascii=False, indent=2)
            os.replace(tmp, self._path)
        except Exception as e:
            sys.stderr.write('保存历史节点池失败: %s\n' % e)

    # ---- 查询 ----
    def __len__(self):
        with self._lock:
            return len(self._nodes)

    def list_nodes(self):
        """按最近速度降序返回节点列表(界面展示用)"""
        with self._lock:
            nodes = sorted(self._nodes.values(),
                           key=lambda e: (-float(e.get('last_speed') or 0), e.get('ipport', '')))
            out = []
            for e in nodes:
                item = dict(e)
                try:
                    item['best_speed_text'] = '%.2fMB/s' % float(e.get('best_speed') or 0)
                except (TypeError, ValueError):
                    item['best_speed_text'] = '-'
                out.append(item)
            return out

    def stable_entries(self, run_id, min_hits=10):
        """返回"本次全量任务达标 且 出现次数 > min_hits"的节点(按出现次数降序)

        判定口径(两者同时满足):
          ① last_seen == run_id —— 该节点在本次全量任务中复测达标("上一次全量任务测试通过")
          ② hits > min_hits     —— 累计达标次数严格大于阈值("出现次数超过 N 次")
        hits 只在节点留在池中时累加, 受滚动窗口影响: 节点连续缺席超过 history_window_runs
        轮会被清出池, 再次出现时 hits 从 1 重新计数。
        排序: 出现次数降序 → 最近速度降序 → ipport 升序(保证顺序稳定可复现)。
        """
        try:
            min_hits = int(min_hits)
        except (TypeError, ValueError):
            min_hits = 10
        with self._lock:
            out = []
            for e in self._nodes.values():
                if str(e.get('last_seen') or '') != str(run_id):
                    continue
                hits = int(e.get('hits') or 0)
                if hits <= min_hits:
                    continue
                item = dict(e)
                item['hits'] = hits
                out.append(item)
        out.sort(key=lambda e: (-e['hits'],
                                -float(e.get('last_speed') or 0),
                                e.get('ipport') or ''))
        return out

    def stats(self):
        with self._lock:
            by_origin = {}
            for e in self._nodes.values():
                name = e.get('origin_source_name') or '未知'
                by_origin[name] = by_origin.get(name, 0) + 1
            return {'total': len(self._nodes),
                    'by_origin': by_origin,
                    'recent_runs': list(self._recent_runs)}

    def clear(self):
        with self._lock:
            self._nodes = {}
            self._recent_runs = []
            self._save()

    # ---- 任务侧 ----
    def build_test_source(self, settings):
        """返回历史伪源描述; 未开启或池为空时返回 None"""
        if not settings.get('history_test_enabled', True):
            return None
        with self._lock:
            count = len(self._nodes)
        if count <= 0:
            return None
        return {'id': HISTORY_SOURCE_ID, 'name': HISTORY_SOURCE_NAME,
                'url': '(节点池 %d 个节点 · 本地文件)' % count,
                'enabled': True, 'is_history': True, 'pool_size': count}

    def export_input_file(self, path):
        """把池内全部节点导出为 ip:port 文本(每行一个), 供 cfdata -nsbfile 复测"""
        with self._lock:
            keys = sorted(self._nodes.keys())
        with open(path, 'w', encoding='utf-8') as f:
            for k in keys:
                f.write(k + '\n')
        return len(keys)

    def update_from_run(self, run_id, api_results, history_rows, history_ok, settings):
        """任务成功后更新池; 返回变化统计

        api_results: [(src_dict, rows)] 各 API 源及其本轮结果行
        history_rows: 历史伪源的复测结果行(未执行时为 None)
        history_ok: 历史伪源本轮是否成功执行(失败则不计失败次数)
        """
        try:
            speed_min = float(settings.get('speed_min') or 0)
        except (TypeError, ValueError):
            speed_min = 0.0
        window = max(1, int(settings.get('history_window_runs') or 5))
        capacity = max(1, int(settings.get('history_pool_capacity') or 250))
        evict_fails = max(1, int(settings.get('history_evict_fails') or 3))

        with self._lock:
            nodes = {k: dict(v) for k, v in self._nodes.items()}
            recent = (self._recent_runs + [run_id])[-window:]
            refreshed = set()
            added = 0

            def _refresh(entry, row):
                entry['last_seen'] = run_id
                entry['fail_streak'] = 0
                entry['last_speed'] = row['speed']
                entry['last_speed_text'] = row['speed_text']
                entry['last_latency'] = row['latency']
                try:
                    entry['best_speed'] = max(float(entry.get('best_speed') or 0), row['speed'])
                except (TypeError, ValueError):
                    entry['best_speed'] = row['speed']

            # 1) API 源达标节点: 入池/刷新(归属在首次入池时固化)
            for src, rows in api_results or []:
                for r in rows:
                    if r['speed'] is None or r['speed'] < speed_min:
                        continue
                    key = r['ipport']
                    e = nodes.get(key)
                    if e is None:
                        nodes[key] = {
                            'ipport': key, 'ip': r['ip'], 'port': r['port'],
                            'origin_source_id': src.get('id', ''),
                            'origin_source_name': src.get('name', '未知源'),
                            'origins': [src.get('name', '未知源')],
                            'origin_run_id': run_id,
                            'first_seen': run_id, 'last_seen': run_id,
                            'last_speed': r['speed'], 'best_speed': r['speed'],
                            'last_speed_text': r['speed_text'], 'last_latency': r['latency'],
                            'hits': 1, 'fail_streak': 0,
                        }
                        added += 1
                    else:
                        if src.get('name') and src['name'] not in (e.get('origins') or []):
                            e.setdefault('origins', []).append(src['name'])
                        if key not in refreshed:
                            e['hits'] = int(e.get('hits') or 0) + 1
                        _refresh(e, r)
                    refreshed.add(key)

            # 2) 历史复测达标: 刷新已有池节点(不改归属; 本轮已被 API 源刷新过的不重复计)
            history_qualified = set()
            for r in (history_rows or []):
                if r['speed'] is None or r['speed'] < speed_min:
                    continue
                key = r['ipport']
                history_qualified.add(key)
                e = nodes.get(key)
                if e is not None and key not in refreshed:
                    e['hits'] = int(e.get('hits') or 0) + 1
                    _refresh(e, r)
                    refreshed.add(key)

            # 3) 历史源成功执行时, 未复测达标的池节点计一次失败; 连续 K 次移出
            evicted = []
            if history_ok:
                for key, e in list(nodes.items()):
                    if key in refreshed or key in history_qualified:
                        continue
                    e['fail_streak'] = int(e.get('fail_streak') or 0) + 1
                    if e['fail_streak'] >= evict_fails:
                        nodes.pop(key, None)
                        evicted.append(key)

            # 4) 滚动窗口: last_seen 不在最近 N 次运行内的节点移出
            window_set = set(recent)
            expired = [k for k, e in nodes.items() if e.get('last_seen') not in window_set]
            for k in expired:
                nodes.pop(k, None)

            # 5) 容量上限: 挤出最近速度最慢的
            dropped = []
            if len(nodes) > capacity:
                ordered = sorted(nodes.items(),
                                 key=lambda kv: (-float(kv[1].get('last_speed') or 0), kv[0]))
                for k, _ in ordered[capacity:]:
                    nodes.pop(k, None)
                    dropped.append(k)

            self._nodes = nodes
            self._recent_runs = recent
            self._save()
            return {'total': len(nodes), 'added': added, 'refreshed': len(refreshed),
                    'evicted': len(evicted), 'expired': len(expired), 'dropped': len(dropped)}


# ---------------------------------------------------------------- Cron 解析
class CronExpr:
    """标准 5 段 cron: 分 时 日 月 周  支持 * , - / 以及 dom/dow OR 语义"""

    def __init__(self, expr):
        self.expr = (expr or '').strip()
        parts = self.expr.split()
        if len(parts) != 5:
            raise ValueError('cron 表达式必须是 5 段: 分 时 日 月 周')
        self.minute = self._parse_field(parts[0], 0, 59)
        self.hour = self._parse_field(parts[1], 0, 23)
        self.dom = self._parse_field(parts[2], 1, 31)
        self.month = self._parse_field(parts[3], 1, 12)
        self.dow = self._parse_field(parts[4], 0, 7)
        if 7 in self.dow:  # 7 = 周日
            self.dow.add(0)
            self.dow.discard(7)
        self.dom_star = parts[2] == '*'
        self.dow_star = parts[4] == '*'

    @staticmethod
    def _parse_field(field, lo, hi):
        values = set()
        for token in field.split(','):
            token = token.strip()
            if not token:
                raise ValueError('cron 字段格式错误: %r' % field)
            step = 1
            if '/' in token:
                token, step_s = token.split('/', 1)
                step = int(step_s)
                if step <= 0:
                    raise ValueError('cron 步长必须为正整数')
            if token == '*':
                start, end = lo, hi
            elif '-' in token:
                a, b = token.split('-', 1)
                start, end = int(a), int(b)
            else:
                start = end = int(token)
                if '/' in field and step > 1 and token != '*':
                    end = hi
            if start < lo or end > hi or start > end:
                raise ValueError('cron 字段取值越界: %r' % field)
            values.update(range(start, end + 1, step))
        return values

    def matches(self, dt):
        if dt.minute not in self.minute or dt.hour not in self.hour or dt.month not in self.month:
            return False
        dom_ok = dt.day in self.dom
        dow_ok = ((dt.weekday() + 1) % 7) in self.dow  # monday=1..sunday=0
        if self.dom_star and self.dow_star:
            return True
        if self.dom_star:
            return dow_ok
        if self.dow_star:
            return dom_ok
        return dom_ok or dow_ok  # 标准 cron: 日与周都受限时取 OR

    def next_runs(self, count=3, after=None):
        base = (after or datetime.now()).replace(second=0, microsecond=0) + timedelta(minutes=1)
        runs = []
        cur = base
        for _ in range(count):
            for _ in range(60 * 24 * 400):  # 最多向后找 ~400 天
                if cur.year > base.year + 2:
                    return runs
                if self.matches(cur):
                    runs.append(cur)
                    cur = cur + timedelta(minutes=1)
                    break
                cur = cur + timedelta(minutes=1)
            else:
                break
        return runs

    def describe(self):
        presets = {
            '*/30 * * * *': '每 30 分钟',
            '0 * * * *': '每小时整点',
            '0 8 * * *': '每天 08:00',
            '0 0 * * *': '每天 00:00',
            '0 8 * * 1': '每周一 08:00',
            '0 9 * * 1-5': '工作日 09:00',
        }
        if self.expr in presets:
            return presets[self.expr]
        return '自定义: ' + self.expr


# ---------------------------------------------------------------- 日志缓冲
# 日志行序号全局单调递增: 每轮任务/质检启动时 _run()/_run_qa() 都会清空日志缓冲,
# 若序号随之归零, 前端 after=<上次的高位序号> 在新一轮里永远读不到任何行 —— 表现为
# 首页日志框卡在旧内容不动(尤其定时触发与质检触发, 因为手动触发时前端会重置游标)。
# 序号不回退即可根治: 新一轮的日志序号必定大于前端已持有的游标。
_LOG_SEQ_LOCK = threading.Lock()
_LOG_SEQ = 0


def _next_log_seq():
    global _LOG_SEQ
    with _LOG_SEQ_LOCK:
        _LOG_SEQ += 1
        return _LOG_SEQ


class LogBuffer:
    def __init__(self, maxlen=4000):
        self._lock = threading.Lock()
        self._lines = []       # [(seq, line)]
        self._maxlen = maxlen

    def clear(self):
        """清空已缓存的行; 序号继续递增, 不回退"""
        with self._lock:
            self._lines = []

    def write(self, line):
        with self._lock:
            seq = _next_log_seq()
            self._lines.append((seq, line))
            if len(self._lines) > self._maxlen:
                self._lines = self._lines[-self._maxlen:]
            return seq

    def read(self, after=0):
        with self._lock:
            return [(s, l) for s, l in self._lines if s > after]


# ---------------------------------------------------------------- 任务执行器
class TaskRunner:
    def __init__(self, store: ConfigStore):
        self.store = store
        self._lock = threading.Lock()
        self._running = False
        self._cancel = False
        self._proc = None
        self.log = LogBuffer()
        self.state = {
            'running': False,
            'phase': 'idle',            # idle / preparing / running / finishing / done / error / canceled / qa
            'kind': '',                 # task / qa: 当前(最近一次)执行的是什么
            'trigger': '',
            'run_id': '',
            'started_at': None,
            'finished_at': None,
            'current_source': '',
            'progress_done': 0,
            'progress_total': 0,
            'message': '',
            # 当前源的子进度: 扫描(延迟测试)与测速
            'scan_done': 0, 'scan_total': 0,
            'speed_tested': 0, 'speed_total': 0, 'speed_qualified': 0,
        }
        # 进度正则: cfdata CLI 输出
        self._re_scan_start = re.compile(r'开始扫描：(\d+)\s*个地址')
        self._re_speed_start = re.compile(r'开始(?:非标)?测速：(\d+)\s*条记录')
        # [speed] [x/9999 0.23%] IP:PORT 速度 —— 分母 9999 是 -nsbspeedlimit(达标上限)而非真实待测总数
        self._re_speed_line = re.compile(r'(\[speed\]\s*)\[(\d+)/(\d+)[\s\d.]*%\]\s*(\S+)\s+(\S+)')
        self._re_speed_done = re.compile(r'(?:非标|官方)批量测速完成，达标\s*(\d+)/(\d+)，总达标\s*(\d+)')
        self._re_scan_line = re.compile(r'\[scan-result\]\s*\[(\d+)/(\d+)[\s\d.]*%\]')

    # ---- 状态 ----
    def snapshot(self):
        with self._lock:
            return dict(self.state)

    def _track_progress(self, text, settings=None):
        """解析 cfdata CLI 输出行, 更新当前源的扫描/测速子进度。

        cfdata 非标测速输出 [speed] [x/9999 x%], 其中分母 9999 是 -nsbspeedlimit
        (达标结果上限)而非实际待测总数 —— 真实总数在 '开始测速：N 条记录' 一行。
        此处把误导性分母重写为真实总数, 使日志与子进度条一致。
        返回值: 重写后的展示文本; None 表示按原文展示。"""
        if not text:
            return None
        try:
            m = self._re_speed_start.search(text)
            if m:
                self._set(speed_total=int(m.group(1)), speed_tested=0, speed_qualified=0)
                return None
            m = self._re_scan_start.search(text)
            if m:
                self._set(scan_total=int(m.group(1)), scan_done=0)
                return None
            m = self._re_speed_done.search(text)
            if m:
                with self._lock:
                    self.state['speed_qualified'] = int(m.group(1))
                return None
            if '[speed]' in text:
                m = self._re_speed_line.search(text)
                try:
                    speed_min = float((settings or {}).get('speed_min') or 0)
                except (TypeError, ValueError):
                    speed_min = 0.0
                with self._lock:
                    st = self.state
                    st['speed_tested'] = st.get('speed_tested', 0) + 1
                    total = st.get('speed_total') or 0
                    tested = st.get('speed_tested') or 0
                    if m:
                        spd = parse_speed_mb(m.group(5) or '')
                        if spd is not None and spd >= speed_min:
                            st['speed_qualified'] = st.get('speed_qualified', 0) + 1
                if m and total > 0:
                    pct = min(100.0, tested / float(total) * 100.0)
                    return self._re_speed_line.sub(lambda _m: '%s[%d/%d %.2f%%] %s %s' % (
                        _m.group(1), tested, total, pct, _m.group(4), _m.group(5)), text, count=1)
                return None
            m = self._re_scan_line.search(text)
            if m:
                with self._lock:
                    self.state['scan_done'] = max(self.state.get('scan_done', 0), int(m.group(1)))
                    self.state['scan_total'] = int(m.group(2)) or self.state.get('scan_total', 0)
        except Exception:
            pass
        return None

    def _reset_sub_progress(self):
        self._set(scan_done=0, scan_total=0,
                  speed_tested=0, speed_total=0, speed_qualified=0)

    def _set(self, **kw):
        with self._lock:
            self.state.update(kw)

    def is_running(self):
        with self._lock:
            return self._running

    # ---- 触发 ----
    def start(self, trigger='manual'):
        # 前置校验: 无已启用源、历史池不可用(关闭/为空)且官方 IPv6 优选未开启时
        # 直接拒绝, 不产生秒失败的运行记录
        cfg = self.store.get()
        official_v6_on = bool((cfg.get('settings') or {}).get('official_v6_enabled'))
        if not self.store.enabled_sources() and \
                not HISTORY.build_test_source(cfg.get('settings') or {}) and \
                not official_v6_on:
            reason = ('历史节点复测已关闭' if not (cfg.get('settings') or {})
                      .get('history_test_enabled', True) else '历史节点池为空')
            return False, ('没有已启用的 API 源, 请先在「API 源管理」中添加'
                           ' (或开启官方 IPv6 优选; %s)' % reason)
        with self._lock:
            if self._running:
                if self.state.get('kind') == 'qa':
                    return False, '质检正在运行中, 请等待完成后再触发'
                return False, '任务正在运行中, 请等待完成后再触发'
            self._running = True
            self._cancel = False
        try:
            t = threading.Thread(target=self._run, args=(trigger,), daemon=True, name='task-runner')
            t.start()
            return True, '任务已启动'
        except Exception as e:
            with self._lock:
                self._running = False
            return False, '任务启动失败: %s' % e

    def start_qa(self, trigger='manual'):
        """质检子任务: 复测 latest 的原始 Top 节点, 低于阈值的从 top_nodes 文件剔除"""
        if not read_qa_input_lines():
            return False, '暂无可质检的节点, 请先成功运行一次任务'
        with self._lock:
            if self._running:
                if self.state.get('kind') == 'qa':
                    return False, '质检正在运行中, 请等待完成后再触发'
                return False, '任务正在运行中, 请等待完成后再触发'
            self._running = True
            self._cancel = False
        try:
            t = threading.Thread(target=self._run_qa, args=(trigger,),
                                 daemon=True, name='qa-runner')
            t.start()
            return True, '质检已启动'
        except Exception as e:
            with self._lock:
                self._running = False
            return False, '质检启动失败: %s' % e

    def cancel(self):
        with self._lock:
            if not self._running:
                return False, '当前没有运行中的任务'
            self._cancel = True
            proc = self._proc
        if proc is not None:
            try:
                proc.terminate()
            except Exception:
                pass
        return True, '已发送取消请求'

    # ---- cfdata 二进制定位与初始化 ----
    def locate_binary(self):
        cfg = self.store.get()
        candidates = []
        p = (cfg['settings'].get('binary_path') or '').strip()
        if p:
            candidates.append(p)
        candidates += [
            os.path.join(APP_DIR, 'cfdata-linux-amd64'),
            os.path.join(APP_DIR, 'cfdata'),
        ]
        which = shutil.which('cfdata')
        if which:
            candidates.append(which)
        for c in candidates:
            if os.path.isfile(c) and os.access(c, os.X_OK):
                return c
        for c in candidates:
            if os.path.isfile(c):
                os.chmod(c, 0o755)
                return c
        return None

    def ensure_cfdata_config(self, binary):
        """cfdata 首次运行会生成配置文件并退出, 这里提前生成避免正式任务被中断"""
        if os.path.exists(CFDATA_CONFIG_PATH):
            return True
        try:
            subprocess.run(
                [binary, '-cli=true', '-config=' + CFDATA_CONFIG_PATH],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                cwd=APP_DIR, timeout=60,
            )
        except Exception:
            pass
        return os.path.exists(CFDATA_CONFIG_PATH)

    # ---- 主流程 ----
    def _run(self, trigger):
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        t0 = time.time()  # 任务总耗时(结束时打印结果汇总用)
        # 清空上一轮日志但保留 LogBuffer 对象本身: 序号持续递增, 前端游标不会失效
        self.log.clear()
        self._set(running=True, kind='task', phase='preparing', trigger=trigger, run_id=run_id,
                  started_at=now_str(), finished_at=None, current_source='',
                  progress_done=0, progress_total=0, message='')
        logf = None
        try:
            cfg = self.store.get()
            settings = cfg['settings']
            sources = self.store.enabled_sources()
            # 历史节点伪源: 池非空且开关开启时, 作为第一个"源"参与本轮测试
            history_src = HISTORY.build_test_source(settings)
            # 官方 IPv6 伪源: 开关开启时附加官方模式(-mode=official -offiptype=6)子任务,
            # 扫描 Cloudflare 官方 IPv6 地址库, 结果与 API 源合并排名
            official_v6_src = None
            if settings.get('official_v6_enabled'):
                official_v6_src = {'name': '官方IPv6优选', 'url': 'official://ipv6', 'is_official_v6': True}
            if not sources and not history_src and not official_v6_src:
                raise RuntimeError('没有已启用的 API 源, 请先在「API 源管理」中添加'
                                   ' (或开启官方 IPv6 优选)')
            binary = self.locate_binary()
            if not binary:
                raise RuntimeError('未找到 cfdata 可执行文件, 请在「参数设置」中配置二进制路径')
            self.ensure_cfdata_config(binary)

            run_dir = os.path.join(RESULTS_DIR, run_id)
            os.makedirs(run_dir, exist_ok=True)
            logf = open(os.path.join(run_dir, 'run.log'), 'a', encoding='utf-8')

            def log(msg):
                line = '[%s] %s' % (now_str('%H:%M:%S'), msg)
                self.log.write(line)
                if logf:
                    logf.write(line + '\n')
                    logf.flush()

            log('===== 任务开始 (触发方式: %s, 运行 ID: %s) =====' % (
                '手动' if trigger == 'manual' else '定时', run_id))
            log('二进制: %s' % binary)
            if sources:
                log('已启用 API 源: %d 个 -> %s' % (
                    len(sources), '、'.join(s['name'] for s in sources)))
            else:
                log('已启用 API 源: 0 个 (仅测试历史节点池)')
            if history_src:
                log('历史节点复测: 已启用, 池内 %d 个节点将作为第一个"源"参与本轮测试' % history_src['pool_size'])
            else:
                log('历史节点复测: 跳过 (%s)' % (
                    '已关闭' if not settings.get('history_test_enabled', True) else '节点池为空'))
            if official_v6_src:
                log('官方 IPv6 优选: 已启用, 阶段1 扫描 Cloudflare 官方 IPv6 地址库'
                    ' (-mode=official -offiptype=6, 延迟阈值 %dms, 覆盖全部机房), '
                    '阶段2 将延迟达标节点写入临时文件按 IP 逐个测速(达标上限 %d), '
                    '结果与 API 源合并排名'
                    % (settings.get('official_v6_delay', 500),
                       max(1, int(settings.get('official_v6_count') or 20))))
            else:
                log('官方 IPv6 优选: 跳过 (未开启)')

            # 订阅模板每日刷新(超过 24h 才重新拉取; 失败沿用缓存)
            if settings.get('sub_convert_enabled') and (settings.get('sub_convert_url') or '').strip():
                _, sub_msg = refresh_sub_template(settings)
                log(sub_msg)
            else:
                log('订阅模板转换: 跳过 (未开启)')

            test_list = (([history_src] if history_src else [])
                         + ([official_v6_src] if official_v6_src else [])
                         + sources)
            self._set(phase='running', progress_total=len(test_list), progress_done=0)

            all_rows = {}
            source_reports = []
            source_rows = []   # 每个源各自的原始行(保留源归属, 用于分源 Top)
            api_results = []   # [(src, rows)] API 源结果, 用于历史池更新
            history_rows, history_ok = None, False
            for idx, src in enumerate(test_list):
                if self._cancel:
                    raise _CanceledError()
                # 显示"第 idx+1 个源进行中", 完成后自然递增
                self._set(current_source=src['name'], progress_done=idx + 1)
                self._reset_sub_progress()
                label = '(%d/%d) %s' % (idx + 1, len(test_list), src['name'])
                rows, report = self._run_one_source(binary, src, idx, run_dir, settings, log, label)
                source_reports.append(report)
                source_rows.append(rows)
                if src.get('is_history'):
                    history_rows, history_ok = rows, bool(report.get('ok'))
                elif src.get('is_official_v6'):
                    # 官方 IPv6 结果仅参与合并排序与分源 Top, 不进历史节点池
                    # (每轮都从官方地址库重新扫描, 无需池化跨轮复测)
                    if not report.get('ok'):
                        log('%s 官方 IPv6 优选未产出达标节点(延迟扫描 0 条或按 IP 测速'
                            ' 全部未达标); 提示: 官方模式需要服务器具备'
                            ' IPv6 出口网络 (Docker 默认桥接网络无 IPv6,'
                            ' 需为容器启用 IPv6 或改用 host 网络后重试)' % label)
                else:
                    api_results.append((src, rows))
                for r in rows:
                    key = r['ipport']
                    if key not in all_rows or r['speed'] > all_rows[key]['speed']:
                        all_rows[key] = r

            self._set(progress_done=len(test_list), phase='finishing', current_source='汇总排序')
            merged = sorted(all_rows.values(),
                            key=lambda r: (-r['speed'], r['latency_ms'], r['ipport']))
            log('全部源测试完成: 共 %d 个有效节点 (已按速度降序排列)' % len(merged))

            # 全部源失败(0 节点)时判任务失败: 不写输出文件、不同步 latest(订阅保持原样),
            # 避免一次全灭把订阅清空(qa_input 质检基准也一并保住)
            if not merged:
                failed = [r.get('name', '?') for r in source_reports if not r.get('ok')]
                raise RuntimeError(
                    '所有源测试失败, 无有效节点 (%s); latest 订阅文件保持原样未动'
                    % ('、'.join(failed) if failed else '无源执行'))

            top_n = int(settings.get('top_n') or 20)
            # ---- 按地址族拆分: IPv4 与 IPv6 各自独立提取 Top(互不挤占名额) ----
            # 判断依据是地址本身(ip 含冒号 = IPv6), 与来源无关: API 源返回的 v6 节点
            # 同样归入 IPv6 侧, 官方优选若产出 v4 也归入 IPv4 侧
            top_v4 = [r for r in merged if ':' not in r['ip']][:top_n]
            top_v6 = [r for r in merged if ':' in r['ip']][:top_n]
            top = top_v4
            log('Top 提取: IPv4 %d 个 (共 %d 候选) / IPv6 %d 个 (共 %d 候选), 各取前 %d'
                % (len(top_v4), sum(1 for r in merged if ':' not in r['ip']),
                   len(top_v6), sum(1 for r in merged if ':' in r['ip']), top_n))
            for i, r in enumerate(top_v4):
                log('Top%-3d %s  速度=%s  延迟=%s  数据中心=%s' % (
                    i + 1, r['ipport'], r['speed_text'], r['latency'], r['dc'] or '-'))
            for i, r in enumerate(top_v6):
                log('TopV6-%-2d %s  速度=%s  延迟=%s  数据中心=%s' % (
                    i + 1, r['ipport'], r['speed_text'], r['latency'], r['dc'] or '-'))

            # ---- 分源 Top N: 每个源(含历史伪源)单独提取速度最快的前 N 个 ----
            per_source_n = max(1, int(settings.get('per_source_top_n') or 5))
            per_source_top = []
            for idx, src in enumerate(test_list):
                rows = sorted((r for r in source_rows[idx] if r['speed'] >= 0),
                              key=lambda r: (-r['speed'], r['latency_ms'], r['ipport']))
                src_top = rows[:per_source_n]
                per_source_top.append({'name': src['name'], 'nodes': src_top})
                log('[%s] 速度最快 %d 个节点:' % (src['name'], len(src_top)))
                for i, r in enumerate(src_top):
                    log('  #%-2d %s  速度=%s  延迟=%s  数据中心=%s' % (
                        i + 1, r['ipport'], r['speed_text'], r['latency'], r['dc'] or '-'))

            # ---- 生成输出文件(IPv4 / IPv6 分开) ----
            paths = self._write_outputs(run_dir, top_v4, top_v6, merged,
                                        per_source_top, settings, log)
            txt_path, yaml_path, all_path = paths['txt'], paths['yaml'], paths['all']
            v6_txt_path, v6_yaml_path, v6_all_path = paths['v6_txt'], paths['v6_yaml'], paths['v6_all']
            by_source_path, by_source_yaml_path = paths['by_source'], paths['by_source_yaml']
            record = {
                'id': run_id,
                'trigger': trigger,
                'started_at': self.state['started_at'],
                'finished_at': now_str(),
                'status': 'success',
                'sources': source_reports,
                'total_nodes': len(merged),
                'top_count': len(top_v4),
                'top_v6_count': len(top_v6),
                'per_source_count': per_source_n,
                'files': {
                    'txt': os.path.basename(txt_path),
                    'yaml': os.path.basename(yaml_path),
                    'all': os.path.basename(all_path),
                    'v6_txt': os.path.basename(v6_txt_path),
                    'v6_yaml': os.path.basename(v6_yaml_path),
                    'v6_all': os.path.basename(v6_all_path),
                    'by_source': os.path.basename(by_source_path),
                    'by_source_yaml': os.path.basename(by_source_yaml_path),
                },
                'top_nodes': [self._node_payload(r, i, settings) for i, r in enumerate(top_v4)],
                'top_v6_nodes': [self._node_payload(r, i, settings) for i, r in enumerate(top_v6)],
                'per_source_top': [
                    {'name': g['name'],
                     'nodes': [self._node_payload(r, i, settings)
                               for i, r in enumerate(g['nodes'])]}
                    for g in per_source_top
                ],
            }
            save_run_record(record)
            self._sync_latest(run_dir, record, settings, log)
            # ---- 历史节点池更新(仅任务成功时; 取消/失败不动池) ----
            if api_results or history_rows is not None:
                try:
                    h = HISTORY.update_from_run(run_id, api_results, history_rows,
                                                history_ok, settings)
                    log('历史节点池已更新: 共 %d 个节点 (本轮新增 %d / 刷新 %d / 淘汰 %d / '
                        '窗口过期 %d / 容量挤出 %d)' % (
                            h['total'], h['added'], h['refreshed'],
                            h['evicted'], h['expired'], h['dropped']))
                except Exception as e:
                    log('历史节点池更新失败: %s' % e)
            # ---- 稳定节点(按出现次数) ----
            # 必须在历史池更新之后生成: 此时"上一次全量任务"就是刚跑完的这一轮,
            # 池内 last_seen == run_id 才等价于"本次全量任务测试通过"
            try:
                self._write_stable_latest(run_id, settings, api_results,
                                          history_rows, log)
            except Exception as e:
                log('稳定节点文件生成失败: %s' % e)
            log('已保存 IPv4 %d 个节点: %s / %s; IPv6 %d 个节点: %s / %s (双栈分开输出)' % (
                len(top_v4), os.path.basename(txt_path), os.path.basename(yaml_path),
                len(top_v6), os.path.basename(v6_txt_path), os.path.basename(v6_yaml_path)))
            log('已生成分源优选结果: %s / %s (每源速度最快 %d 个)' % (
                os.path.basename(by_source_path), os.path.basename(by_source_yaml_path), per_source_n))
            # 把最终结果清单打印到日志(可直接查看/复制, 不必下载文件)
            self._log_final_summary(log, record, top_v4, top_v6, source_reports,
                                    time.time() - t0, settings)
            log('===== 任务完成 =====')
            self._set(phase='done', running=False, finished_at=now_str(),
                      current_source='', message='成功: IPv4 %d 个 / IPv6 %d 个节点'
                      % (len(top_v4), len(top_v6)))
        except _CanceledError:
            msg = '任务已取消'
            self.log.write('[%s] %s' % (now_str('%H:%M:%S'), msg))
            self._finish_run_record(run_id, 'canceled')
            self._set(phase='canceled', running=False, finished_at=now_str(), message=msg)
        except Exception as e:
            msg = '任务失败: %s' % e
            self.log.write('[%s] %s' % (now_str('%H:%M:%S'), msg))
            self._finish_run_record(run_id, 'error')
            self._set(phase='error', running=False, finished_at=now_str(), message=msg)
        finally:
            if logf:
                logf.close()
            with self._lock:
                self._running = False
                self._proc = None

    def _log_final_summary(self, log, record, top_v4, top_v6, source_reports, elapsed, settings):
        """任务成功结束时把最终结果清单打印到日志

        此前日志只有统计摘要, 想看具体入选节点必须下载文件。这里按与输出文件
        完全一致的格式(ip:port#速度-数据中心-位置)逐行打印最终 Top 节点, 便于在
        页面日志里直接查看与复制; 节点过多时按 SUMMARY_MAX_LINES 截断并提示,
        完整内容仍以 results/ 下的输出文件为准。
        """
        log('=' * 40)
        log('最终结果汇总 | 运行 ID %s | 触发方式 %s | 总耗时 %.1f 秒'
            % (record.get('id', ''),
               '手动' if record.get('trigger') == 'manual' else '定时', elapsed))
        log('有效节点 %d 个 -> 提取 IPv4 %d 个 / IPv6 %d 个 (每栈上限 %d)'
            % (record.get('total_nodes', 0), len(top_v4), len(top_v6),
               int(settings.get('top_n') or 20)))
        # 各源执行情况: 成功/失败/测速未达标三种口径
        for r in source_reports or []:
            if r.get('ok'):
                status = '成功'
            elif r.get('error'):
                status = '失败'
            else:
                status = '无达标节点'
            log('  · %s: %s, %d 个节点, 耗时 %.1f 秒, 尝试 %d 次'
                % (r.get('name', '?'), status, r.get('count', 0),
                   float(r.get('elapsed_sec') or 0), r.get('attempts', 1)))
            if r.get('error'):
                log('      原因: %s' % r['error'])
        self._log_node_group(log, 'IPv4 Top %d (top_nodes.txt)' % len(top_v4), top_v4)
        self._log_node_group(log, 'IPv6 Top %d (top_nodes_v6.txt)' % len(top_v6), top_v6)
        log('输出目录: results/latest/ (top_nodes.* / top_nodes_v6.* / '
            'all_sorted*.txt / top_by_source.*)')
        log('=' * 40)

    def _log_node_group(self, log, title, rows):
        """把一批节点逐行打印到日志, 格式与输出文件一致(ip:port#速度-数据中心-位置);
        行数超过 SUMMARY_MAX_LINES 时截断并提示, 避免日志缓冲被挤满"""
        if not rows:
            return
        lines = ['%s#%s-%s-%s' % (r['ipport'], r['speed_text'],
                                  r['dc'] or 'CF', r['loc'] or 'XX') for r in rows]
        log('---- %s ----' % title)
        for ln in lines[:SUMMARY_MAX_LINES]:
            log(ln)
        if len(lines) > SUMMARY_MAX_LINES:
            log('  …… 其余 %d 个节点见输出文件' % (len(lines) - SUMMARY_MAX_LINES))

    def _log_qa_summary(self, log, run_id, trigger, elapsed, checked, final_rows,
                        pruned, revived, failed):
        """质检结束时把保留/剔除结果打印到日志(与主任务的结果汇总块风格一致)

        保留清单按地址族拆分为 v4 / v6 两段, 与刚重写进 latest 的订阅文件内容一致;
        剔除与回归清单各限行数, 完整明细以上方的逐行日志为准。
        """
        kept_v4 = [r for r in final_rows if ':' not in r['ip']]
        kept_v6 = [r for r in final_rows if ':' in r['ip']]
        log('=' * 40)
        log('质检结果 | 运行 ID %s | 触发方式 %s | 总耗时 %.1f 秒'
            % (run_id, '手动' if trigger == 'manual' else '定时', elapsed))
        log('检查 %d 个 -> 保留 %d 个 (IPv4 %d / IPv6 %d) | 剔除 %d 个 | 回归 %d 个'
            % (checked, len(final_rows), len(kept_v4), len(kept_v6),
               len(pruned or []), len(revived or [])))
        self._log_node_group(log, '保留节点 IPv4 %d (已写入 top_nodes.txt)' % len(kept_v4), kept_v4)
        self._log_node_group(log, '保留节点 IPv6 %d (已写入 top_nodes_v6.txt)' % len(kept_v6), kept_v6)
        if pruned:
            failed_set = set(failed or [])
            log('剔除清单 %d 个 (仍留在基准中, 恢复达标后自动回归):' % len(pruned))
            for p in pruned[:QA_PRUNE_MAX_LINES]:
                spd = p.get('speed') or '-'
                # 未出现在结果文件 = 本次未达标/失联(该行 speed 已置为提示文案, 不再重复原因)
                if p.get('ipport') in failed_set or spd == '未出现在结果中':
                    log('  · %s (未出现在结果中)' % p.get('ipport'))
                else:
                    log('  · %s (%s, 低于阈值)' % (p.get('ipport'), spd))
            if len(pruned) > QA_PRUNE_MAX_LINES:
                log('  …… 其余 %d 个见上方逐行日志' % (len(pruned) - QA_PRUNE_MAX_LINES))
        if revived:
            names = [v.get('ipport') for v in revived[:QA_PRUNE_MAX_LINES]]
            log('回归清单 %d 个 (速度恢复达标, 已重新写回订阅): %s%s'
                % (len(revived), '、'.join(names),
                   ' 等' if len(revived) > QA_PRUNE_MAX_LINES else ''))
        log('订阅文件已重写: results/latest/ (top_nodes.* / top_nodes_v6.*)')
        log('=' * 40)

    def _finish_run_record(self, run_id, status):
        try:
            rec = load_run_record(run_id)
            if rec:
                rec['status'] = status
                rec['finished_at'] = now_str()
                save_run_record(rec)
        except Exception:
            pass

    # ---- 单个源测试 ----
    def _run_one_source(self, binary, src, idx, run_dir, settings, log, label):
        """执行单个源测试; 执行失败(退出码非 0 / 异常)时自动重试

        CLI 正常完成(退出码 0 且无异常)即视为本次执行结束 —— 即使 0 个节点测速
        达标(全部未达标)也不重新执行该源: 速度是客观测量结果, 重测只会
        重复消耗时间与带宽, 不会让未达标节点变快; 只有真正的执行失败才重试。
        """
        def _num(key, default, minimum):
            try:
                v = settings.get(key)
                v = default if v is None else float(v)
            except (TypeError, ValueError):
                v = default
            return max(minimum, v)

        retry_times = int(_num('source_retries', 3, 0))
        retry_delay = _num('source_retry_delay', 5, 0)
        total_attempts = retry_times + 1

        started = time.time()
        attempt, rows, code, err = 0, [], None, None
        while True:
            attempt += 1
            rows, code, err = self._exec_source_once(binary, src, idx, run_dir, settings, log, label)
            if self._cancel:
                raise _CanceledError()
            # 执行成功(退出码 0 且无异常)即完成: 测速未达标(0 个达标节点)不触发重试
            exec_ok = (code == 0) and not err
            if exec_ok or attempt >= total_attempts:
                break
            log('%s 第 %d/%d 次尝试失败 (退出码 %s, 有效节点 %d 个), %.0f 秒后重试…' % (
                label, attempt, total_attempts,
                code if code is not None else '异常', len(rows), retry_delay))
            # 分片睡眠, 重试等待期间仍可取消任务
            steps = max(1, int(retry_delay * 2))
            for _ in range(steps):
                if self._cancel:
                    raise _CanceledError()
                time.sleep(max(0.05, retry_delay / steps))

        elapsed = time.time() - started
        if (code == 0) and len(rows) > 0 and not err:
            log('%s 完成: 耗时 %.1f 秒, 尝试 %d 次, 符合条件节点 %d 个' % (
                label, elapsed, attempt, len(rows)))
            return rows, {
                'name': src['name'], 'url': src['url'], 'ok': True,
                'count': len(rows), 'elapsed_sec': round(elapsed, 1),
                'exit_code': code, 'attempts': attempt,
            }
        if (code == 0) and not err:
            # CLI 正常完成但 0 个节点测速达标: 不重试, 按无有效节点口径上报
            log('%s 完成: 耗时 %.1f 秒, 尝试 %d 次, 测速全部未达标 '
                '(0 个达到阈值, 不重试)' % (label, elapsed, attempt))
            return rows, {
                'name': src['name'], 'url': src['url'], 'ok': False,
                'count': 0, 'elapsed_sec': round(elapsed, 1),
                'error': '测速全部未达标, 无有效节点 (CLI 正常完成, 不重试)',
                'exit_code': code, 'attempts': attempt,
            }
        # 全部尝试均失败(返回可能残留的部分结果)
        reason = err or ('退出码 %s, 无有效节点' % (code if code is not None else '未知'))
        if attempt > 1:
            reason = '已重试 %d 次仍失败: %s' % (attempt - 1, reason)
        log('%s 最终失败: %s' % (label, reason))
        return rows, {
            'name': src['name'], 'url': src['url'], 'ok': False,
            'count': len(rows), 'elapsed_sec': round(elapsed, 1),
            'error': reason, 'exit_code': code, 'attempts': attempt,
        }

    def _build_cmd(self, binary, settings, out_name, input_name=None, source_url=None):
        """构造 cfdata 非标测速命令; input_name 走本地文件(-nsbfile), source_url 走网络源"""
        cmd = [
            binary,
            '-cli=true',
            '-mode=nsb',
            '-nsbthreads=%d' % int(settings.get('threads') or 100),
            '-nsbspeedtest=%d' % int(settings.get('speedtest_threads') or 5),
            '-nsbspeedmin=%s' % settings.get('speed_min', 5),
            '-nsbspeedlimit=%d' % int(settings.get('speed_limit') or 9999),
            '-nsbresultlimit=%d' % int(settings.get('result_limit') or 1000),
            '-nsbqualified=%s' % ('true' if settings.get('qualified', True) else 'false'),
            '-nsbtls=%s' % ('true' if settings.get('tls', True) else 'false'),
            '-nsbiptype=%s' % settings.get('ip_type', 'all'),
            '-skipgeo=%s' % ('true' if settings.get('skip_geo_check', True) else 'false'),
            '-format=csv',
            '-fields=%s' % CSV_FIELDS,
            '-nsbout=%s' % out_name,
            '-nocolor=true',
            '-config=%s' % CFDATA_CONFIG_PATH,
        ]
        if input_name:
            cmd.insert(3, '-nsbfile=%s' % input_name)
        elif source_url:
            cmd.insert(3, '-nsbsourceurl=' + source_url)
        return cmd

    def _build_official_cmd(self, binary, settings, out_name):
        """构造官方 IPv6 阶段1 延迟扫描命令 (-mode=official -offiptype=6)

        官方模式原生流程是"扫描 → 选定单一机房(-offdc 不填时自动选延迟最低者)
        → 只对该机房做详细测试与测速", 其余机房(如洛杉矶)的延迟达标节点不会
        获得测速机会。因此这里传入必然不存在的机房代号(-offdc=NONE)并关闭官方
        测速(-offspeedlimit=0), 使 CLI 只做延迟扫描, 把全部延迟达标节点(覆盖
        所有机房, 含数据中心/位置信息, 无速度值)导出为 CSV; 平台随后把这些节点
        写入临时文件, 用非标模式(-nsbfile)按 IP 逐个测速, 所有机房机会均等。
        输出文件用 -offout 指定(官方模式专用参数, 非通用 -out)。
        """
        try:
            delay = max(1, int(settings.get('official_v6_delay') or 500))
        except (TypeError, ValueError):
            delay = 500
        return [
            binary,
            '-cli=true',
            '-mode=official',
            '-offiptype=6',
            '-offout=%s' % out_name,
            '-offthreads=%d' % int(settings.get('threads') or 100),
            '-offport=443',
            '-offdelay=%d' % delay,
            '-offdc=NONE',
            '-offspeedlimit=0',
            '-format=csv',
            '-fields=%s' % CSV_FIELDS,
            '-nocolor=true',
            '-config=%s' % CFDATA_CONFIG_PATH,
        ]

    def _exec_cmd(self, cmd, work_dir, out_path, settings, log, label):
        """在工作目录执行一次 cfdata 命令(流式读日志/超时/取消), 返回 (rows, exit_code, error)"""
        proc = None
        # 复用已缓存的 locations.json / GeoLite2-ASN.mmdb, 避免每次运行都在
        # 任务目录里重复下载(存在即复用, 首次缺失时才真正下载并回写缓存)
        cached_locations = LOCATIONS_CACHE_PATH
        if os.path.exists(cached_locations):
            try:
                shutil.copyfile(cached_locations, os.path.join(work_dir, 'locations.json'))
            except Exception:
                pass
        if os.path.exists(MMDB_CACHE_PATH):
            try:
                shutil.copyfile(MMDB_CACHE_PATH, os.path.join(work_dir, MMDB_NAME))
                log('复用缓存的 %s (data 目录, 跳过下载)' % MMDB_NAME)
            except Exception:
                pass
        strict_geo = not bool(settings.get('skip_geo_check', True))
        try:
            # 严格模式下不给输入(交互确认将默认取消); 跳过模式下自动应答 y 作为兜底
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=work_dir,
                stdin=subprocess.DEVNULL if strict_geo else subprocess.PIPE)
            if not strict_geo:
                try:
                    proc.stdin.write(b'y\n')
                    proc.stdin.flush()
                    proc.stdin.close()
                except Exception:
                    pass
            with self._lock:
                self._proc = proc
            timeout = float(settings.get('timeout_minutes') or 0) * 60
            self._stream_output(proc, log, label, timeout, settings)
            # 超时/取消时 _stream_output 已发过 SIGTERM, 这里必须带超时等待:
            # 子进程若不响应 SIGTERM, 无参数的 wait() 会永久阻塞 -> 执行锁无法
            # 释放 -> 后续任务与质检全部被拒。等待超时后升级为 SIGKILL。
            try:
                code = proc.wait(timeout=PROC_TERM_WAIT)
            except subprocess.TimeoutExpired:
                log('%s 进程在 %d 秒内未响应终止信号, 强制结束' % (label, PROC_TERM_WAIT))
                try:
                    proc.kill()
                    code = proc.wait(timeout=PROC_KILL_WAIT)
                except Exception:
                    log('%s 进程强制结束失败, 本次按失败处理' % label)
                    code = None
            if self._cancel:
                raise _CanceledError()
            if code != 0:
                # code 为 None 表示强制结束也未拿到退出码, 按失败处理
                log('%s 进程退出码 %s' % (label, '未知(强制结束)' if code is None else code))
            rows = self._parse_csv(out_path)
            # 回写缓存 locations.json / GeoLite2-ASN.mmdb, 供后续运行复用(避免重复下载)
            run_locations = os.path.join(work_dir, 'locations.json')
            if os.path.exists(run_locations):
                try:
                    shutil.copyfile(run_locations, cached_locations)
                except Exception:
                    pass
            run_mmdb = os.path.join(work_dir, MMDB_NAME)
            if os.path.exists(run_mmdb):
                try:
                    shutil.copyfile(run_mmdb, MMDB_CACHE_PATH)
                except Exception:
                    pass
            return rows, code, None
        except _CanceledError:
            raise
        except Exception as e:
            log('%s 执行异常: %s' % (label, e))
            rows = self._parse_csv(out_path) if os.path.exists(out_path) else []
            return rows, None, str(e)
        finally:
            with self._lock:
                self._proc = None
            if proc is not None and proc.poll() is None:
                try:
                    proc.kill()
                except Exception:
                    pass
            # 关闭 stdout 管道: 长期运行的服务每跑一次任务就泄漏一个 fd,
            # 累积到上限后 Popen 会失败(每个源最多泄漏 2 个: stdout + stdin)
            if proc is not None:
                for stream in (proc.stdout, proc.stdin):
                    try:
                        if stream is not None and not stream.closed:
                            stream.close()
                    except Exception:
                        pass

    def _exec_source_once(self, binary, src, idx, run_dir, settings, log, label):
        """执行一次 cfdata 测试, 返回 (rows, exit_code, error)

        普通源走 -nsbsourceurl 网络拉取; 历史伪源把池内节点导出为本地文件走 -nsbfile;
        官方 IPv6 伪源分两阶段: 官方模式延迟扫描全部机房(阶段1) → 临时文件按 IP 逐个
        测速(阶段2, -nsbfile)。
        """
        out_name = 'source_%02d.csv' % (idx + 1)
        out_path = os.path.join(run_dir, out_name)
        if os.path.exists(out_path):
            os.remove(out_path)  # 清理上次尝试的残留, 避免误读旧结果
        if src.get('is_official_v6'):
            # 官方 IPv6 伪源分两阶段执行:
            # 阶段1 官方模式仅做延迟扫描(-offdc=NONE 跳过单机房收敛), 导出全部
            #      延迟达标节点(覆盖所有机房, 含数据中心/位置, 无速度值);
            # 阶段2 把这些节点写入临时文件 official_v6_input.txt, 用非标模式
            #      (-nsbfile)按 IP 逐个测速 —— 香港之外(洛杉矶等)的机房同样获得
            #      测速机会, 不再被官方模式"自动选最低延迟机房"过滤掉。
            scan_name = 'official_v6_scan.csv'
            scan_path = os.path.join(run_dir, scan_name)
            if os.path.exists(scan_path):
                os.remove(scan_path)  # 清理上次尝试的残留, 避免误读旧结果
            try:
                delay = max(1, int(settings.get('official_v6_delay') or 500))
            except (TypeError, ValueError):
                delay = 500
            scan_cmd = self._build_official_cmd(binary, settings, scan_name)
            log('%s 开始测试: Cloudflare 官方 IPv6 地址库 阶段1 延迟扫描 '
                '(-mode=official -offiptype=6, 延迟阈值 %dms, 覆盖全部机房, 不做官方单机房测速)'
                % (label, delay))
            scan_rows, code, err = self._exec_cmd(scan_cmd, run_dir, scan_path,
                                                  settings, log, label)
            if (code is not None and code != 0) or err:
                return [], code, err or '官方 IPv6 阶段1 延迟扫描失败 (退出码 %s)' % code
            if not scan_rows:
                # 延迟扫描 0 条(无 IPv6 出口或全部超过延迟阈值): 走既有的
                # "未产出达标节点"口径, 不进入阶段2
                return [], 0, None
            seen, ipports, dcs = set(), [], set()
            for r in scan_rows:
                ipport = bracket_v6_ipport((r.get('ipport') or '').strip())
                if not ipport or ipport in seen:
                    continue
                seen.add(ipport)
                ipports.append(ipport)
                dc = (r.get('dc') or '').strip()
                if dc:
                    dcs.add(dc)
            input_name = 'official_v6_input.txt'
            with open(os.path.join(run_dir, input_name), 'w', encoding='utf-8') as f:
                f.write('\n'.join(ipports) + '\n')
            log('%s 官方 IPv6 阶段2 按IP测速: 延迟达标 %d 条(覆盖 %d 个机房: %s), '
                '已生成临时文件 %s, 非标模式逐个测速'
                % (label, len(ipports), len(dcs), ' / '.join(sorted(dcs)[:10]) or '未知',
                   input_name))
            try:
                count = max(1, int(settings.get('official_v6_count') or 20))
            except (TypeError, ValueError):
                count = 20
            nsb_settings = dict(settings)
            nsb_settings['speed_limit'] = count  # 测速达标上限沿用官方 v6 配置
            cmd = self._build_cmd(binary, nsb_settings, out_name, input_name=input_name)
            cmd.insert(3, '-nsbdelay=%d' % delay)  # 阶段2 延迟复测阈值与阶段1保持一致
            return self._exec_cmd(cmd, run_dir, out_path, settings, log, label)
        if src.get('is_history'):
            # 历史伪源: 池内节点导出为本地 ip:port 文件, 复用同一套测速参数
            input_name = 'history_input.txt'
            input_path = os.path.join(run_dir, input_name)
            try:
                count = HISTORY.export_input_file(input_path)
            except Exception as e:
                return [], 1, '导出历史节点输入文件失败: %s' % e
            cmd = self._build_cmd(binary, settings, out_name, input_name=input_name)
            log('%s 开始测试: 历史节点池 %d 个节点 (本地文件 %s)' % (label, count, input_name))
        else:
            cmd = self._build_cmd(binary, settings, out_name, source_url=src['url'])
            log('%s 开始测试: %s' % (label, src['url']))
        return self._exec_cmd(cmd, run_dir, out_path, settings, log, label)

    def _stream_output(self, proc, log, label, timeout_sec, settings=None):
        """流式读取子进程输出, 兼容 \\r 进度刷新, 带超时与取消"""
        fd = proc.stdout.fileno()
        import select
        buf = b''
        deadline = (time.time() + timeout_sec) if timeout_sec > 0 else None
        last_keepalive = time.time()
        while True:
            if self._cancel:
                try:
                    proc.terminate()
                except Exception:
                    pass
                raise _CanceledError()
            if deadline is not None and time.time() > deadline:
                log('%s 超过超时时间, 终止该源的测试' % label)
                try:
                    proc.terminate()
                except Exception:
                    pass
                return
            r, _, _ = select.select([fd], [], [], 1.0)
            if not r:
                if proc.poll() is not None:
                    break
                continue
            try:
                chunk = os.read(fd, 8192)
            except OSError:
                break
            if not chunk:
                break
            buf += chunk
            parts = re.split(b'\r\n|\r|\n', buf)
            buf = parts.pop()
            for p in parts:
                text = p.decode('utf-8', errors='replace').strip()
                if text:
                    display = self._track_progress(text, settings)
                    log('%s | %s' % (label, display if display is not None else text))
            if time.time() - last_keepalive > 30:
                last_keepalive = time.time()
        # 冲刷剩余
        text = buf.decode('utf-8', errors='replace').strip()
        if text:
            display = self._track_progress(text, settings)
            log('%s | %s' % (label, display if display is not None else text))

    @staticmethod
    def _parse_csv(path):
        """解析 cfdata 导出的 CSV (UTF-8 BOM + 中文表头)"""
        rows = []
        if not os.path.exists(path):
            return rows
        try:
            with open(path, 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.DictReader(f)
                for raw in reader:
                    ipport = (raw.get(HEADER_IPPORT) or '').strip()
                    if not ipport or ':' not in ipport:
                        continue
                    ip, port_s = ipport.rsplit(':', 1)
                    if len(ip) > 2 and ip.startswith('[') and ip.endswith(']'):
                        ip = ip[1:-1]  # 去掉 IPv6 的方括号(如 [2606:4700::1]:443), Clash server 字段用裸地址
                    if ':' in ip and port_s.isdigit():
                        # cfdata CSV 的 IPv6 ipport 为裸拼接(2606:4700::1:443), 统一
                        # 规范化为方括号格式 [2606:4700::1]:443: txt/质检/历史池输入
                        # 都以此为准(裸格式会让 nsb 输入解析丢失端口, 回退 443)
                        ipport = '[%s]:%s' % (ip, port_s)
                    speed_text = (raw.get(HEADER_SPEED) or '').strip()
                    speed = parse_speed_mb(speed_text)
                    rows.append({
                        'ip': ip, 'port': port_s, 'ipport': ipport,
                        'speed': speed if speed is not None else -1.0,
                        'speed_text': speed_text or '未测速',
                        'latency': (raw.get(HEADER_LATENCY) or '').strip(),
                        'latency_ms': parse_latency_ms(raw.get(HEADER_LATENCY) or ''),
                        'dc': (raw.get(HEADER_DC) or '').strip(),
                        'loc': (raw.get(HEADER_LOC) or '').strip(),
                        'region': (raw.get(HEADER_REGION) or '').strip(),
                        'city': (raw.get(HEADER_CITY) or '').strip(),
                    })
        except Exception as e:
            sys.stderr.write('解析 CSV 失败 %s: %s\n' % (path, e))
        return rows

    # ---- 输出生成 ----
    def _yaml_node(self, f, r, name, node_cfg):
        """向已打开的 YAML 文件写入一个 Clash 代理节点块"""
        f.write('  - name: %s\n' % yaml_value(name))
        f.write('    server: %s\n' % yaml_value(r['ip']))
        f.write('    port: %s\n' % r['port'])
        f.write('    type: trojan\n')
        f.write('    password: %s\n' % yaml_value(node_cfg.get('password', '')))
        f.write('    sni: %s\n' % yaml_value(node_cfg.get('sni', '')))
        f.write('    client-fingerprint: %s\n' % yaml_value(node_cfg.get('client_fingerprint', 'chrome')))
        f.write('    skip-cert-verify: %s\n' % ('true' if node_cfg.get('skip_cert_verify') else 'false'))
        f.write('    network: ws\n')
        f.write('    ws-opts:\n')
        f.write('      path: %s\n' % yaml_value(node_cfg.get('path', '/')))
        f.write('      headers:\n')
        f.write('        Host: %s\n' % yaml_value(node_cfg.get('host', node_cfg.get('sni', ''))))

    def _node_fields(self, r, settings):
        dc = r['dc'] or 'CF'
        loc = r['loc'] or 'XX'
        region = r['region'] or ''
        return {
            'dc': dc, 'loc': loc, 'region': region, 'city': r['city'] or '',
            'speed': '%.2f' % max(r['speed'], 0),
            'latency': r['latency'].replace('ms', '') or '0',
            'ip': r['ip'], 'port': r['port'],
        }

    def _node_name(self, r, settings, used):
        node_cfg = settings.get('node', {})
        fields = self._node_fields(r, settings)
        lang = (node_cfg.get('name_lang') or 'zh').lower()
        if lang == 'zh':
            fields = localize_node_fields(fields)
        name = render_name_template(node_cfg.get('name_template') or '{dc}-{loc}-{speed}MB/s', fields)
        name = name.strip() or ('%s:%s' % (r['ip'], r['port']))
        base, i = name, 2
        while name in used:
            name = '%s #%d' % (base, i)
            i += 1
        used.add(name)
        return name

    def _node_payload(self, r, idx, settings):
        used = set()
        node_cfg = settings.get('node', {})
        name = self._node_name(r, settings, used)
        return {
            'rank': idx + 1, 'name': name, 'server': r['ip'], 'port': r['port'],
            'speed': r['speed_text'], 'latency': r['latency'],
            'dc': r['dc'], 'loc': r['loc'],
        }

    def _sync_latest(self, run_dir, record, settings, log):
        """将最新一次成功任务的结果同步到 results/latest/ (固定路径, 内容随每次任务更新)

        启用订阅模板转换时, latest 的两个 Top YAML 在片段之上再生成完整 Clash
        订阅(模板策略组/规则保留, 节点替换为本轮实测 Top), 订阅链接路径不变。
        """
        try:
            os.makedirs(LATEST_DIR, exist_ok=True)
            for fname in LATEST_FILES:
                src = os.path.join(run_dir, fname)
                if os.path.isfile(src):
                    shutil.copyfile(src, os.path.join(LATEST_DIR, fname))
            # 订阅模板转换: latest 的两个 Top YAML 由片段升级为完整 Clash 订阅
            # (未启用/模板缺失/转换失败时保持片段格式, 不影响同步)
            sub_converted = []
            if settings.get('sub_convert_enabled'):
                tpl_text = load_sub_template_text()
                if tpl_text:
                    for yaml_name, key in (('top_nodes.yaml', 'top_nodes'),
                                           ('top_nodes_v6.yaml', 'top_v6_nodes')):
                        entries, used = [], set()
                        for n in record.get(key) or []:
                            name = (n.get('name') or '').strip() \
                                or '%s:%s' % (n.get('server'), n.get('port'))
                            base, i = name, 2
                            while name in used:  # Clash 节点名必须唯一
                                name = '%s #%d' % (base, i)
                                i += 1
                            used.add(name)
                            entries.append((name, n.get('server'), n.get('port')))
                        if not entries:
                            continue
                        try:
                            text = render_clash_subscription(tpl_text, entries)
                            dst = os.path.join(LATEST_DIR, yaml_name)
                            tmp = dst + '.tmp'
                            with open(tmp, 'w', encoding='utf-8') as f:
                                f.write(text)
                            os.replace(tmp, dst)
                            sub_converted.append(yaml_name)
                            log('latest/%s 已按订阅模板转换为完整 Clash 订阅 (%d 个节点)'
                                % (yaml_name, len(entries)))
                        except Exception as e:
                            log('latest/%s 订阅转换失败 (%s), 保留片段格式' % (yaml_name, e))
                else:
                    log('订阅模板缓存不存在, latest Top YAML 保持片段格式 '
                        '(可先手动刷新模板, 再触发一次任务生成完整订阅)')
            # 原始 Top 节点基准: 质检每次都从这份完整列表复测, 不随剔除缩小
            # (v4 与 v6 双栈合并, 质检时一并复测保鲜)
            src_top = os.path.join(run_dir, 'top_nodes.txt')
            src_top_v6 = os.path.join(run_dir, 'top_nodes_v6.txt')
            if os.path.isfile(src_top) or os.path.isfile(src_top_v6):
                with open(QA_INPUT_PATH, 'w', encoding='utf-8') as f:
                    for path in (src_top, src_top_v6):
                        if not os.path.isfile(path):
                            continue
                        with open(path, 'r', encoding='utf-8') as sf:
                            f.write(sf.read().rstrip('\n') + '\n')
            meta = {
                'run_id': record.get('id'),
                'finished_at': record.get('finished_at'),
                'trigger': record.get('trigger'),
                'top_count': record.get('top_count', 0),
                'top_v6_count': record.get('top_v6_count', 0),
                'total_nodes': record.get('total_nodes', 0),
                'per_source_count': record.get('per_source_count', 0),
                'sub_converted': sub_converted,
                'files': {'txt': 'top_nodes.txt', 'yaml': 'top_nodes.yaml',
                          'all': 'all_sorted.txt',
                          'v6_txt': 'top_nodes_v6.txt', 'v6_yaml': 'top_nodes_v6.yaml',
                          'v6_all': 'all_sorted_v6.txt',
                          'by_source': 'top_by_source.txt',
                          'by_source_yaml': 'top_by_source.yaml'},
            }
            tmp = os.path.join(LATEST_DIR, 'meta.json.tmp')
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
            os.replace(tmp, os.path.join(LATEST_DIR, 'meta.json'))
            log('已同步最新结果到固定目录: results/latest/ '
                '(IPv4: top_nodes.txt / top_nodes.yaml / all_sorted.txt; '
                'IPv6: top_nodes_v6.txt / top_nodes_v6.yaml / all_sorted_v6.txt; '
                'top_by_source.* / qa_input.txt)')
        except Exception as e:
            log('同步 latest 目录失败: %s' % e)

    # ---- 稳定节点(按出现次数) ----
    def _write_stable_latest(self, run_id, settings, api_results, history_rows, log):
        """按出现次数提取稳定节点, 写入 results/latest/top_stable.*

        入选需同时满足: ①本次全量任务复测达标(池内 last_seen == 本轮 run_id)
        ②累计出现次数 > stable_min_hits(默认 10, 即至少 11 次)。
        按出现次数降序, IPv4 / IPv6 分开输出, 各取前 top_n 个, 命名与 top_nodes 系列一致。

        无符合条件、或某个协议族为空时保留上次文件不动, 避免把订阅清空。
        """
        try:
            min_hits = int(settings.get('stable_min_hits') or 10)
        except (TypeError, ValueError):
            min_hits = 10
        entries = HISTORY.stable_entries(run_id, min_hits)
        if not entries:
            log('稳定节点: 本轮无节点满足"本次全量任务达标 且 出现次数 > %d", '
                '保留上次 top_stable.* 文件' % min_hits)
            return
        # 本轮实测行(ipport -> row): 补全 dc/loc/速度等字段, 复用统一的节点命名与 YAML 输出
        row_by_key = {}
        for _src, rows in (api_results or []):
            for r in rows:
                row_by_key[r['ipport']] = r
        for r in (history_rows or []):
            row_by_key.setdefault(r['ipport'], r)

        top_n = max(1, int(settings.get('top_n') or 20))
        node_cfg = settings.get('node', {})
        v4, v6, missing = [], [], 0
        for e in entries:
            if len(v4) >= top_n and len(v6) >= top_n:
                break
            row = row_by_key.get(e.get('ipport'))
            if row is None:      # 极少数: 池内记录与本轮结果行未对上
                missing += 1
                continue
            if ':' in row['ip']:
                if len(v6) < top_n:
                    v6.append(row)
            else:
                if len(v4) < top_n:
                    v4.append(row)

        written = []
        for rows, txt_name, yaml_name in ((v4, 'top_stable.txt', 'top_stable.yaml'),
                                          (v6, 'top_stable_v6.txt', 'top_stable_v6.yaml')):
            if not rows:
                continue         # 该协议族无节点: 保留上次文件, 不写空订阅
            os.makedirs(LATEST_DIR, exist_ok=True)
            txt_path = os.path.join(LATEST_DIR, txt_name)
            tmp = txt_path + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                for r in rows:
                    f.write('%s#%s-%s-%s\n' % (r['ipport'], r['speed_text'],
                                               r['dc'] or 'CF', r['loc'] or 'XX'))
            os.replace(tmp, txt_path)
            used = set()
            yaml_path = os.path.join(LATEST_DIR, yaml_name)
            tmp = yaml_path + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                f.write('proxies:\n')
                for r in rows:
                    self._yaml_node(f, r, self._node_name(r, settings, used), node_cfg)
            os.replace(tmp, yaml_path)
            written.append('%s / %s (%d 个)' % (txt_name, yaml_name, len(rows)))

        if written:
            log('稳定节点(出现次数 > %d 且本次全量任务达标): 已写入 latest/ — %s '
                '(按出现次数降序, 各取前 %d%s)' % (
                    min_hits, '; '.join(written), top_n,
                    '; %d 个池内节点未匹配到本轮结果行已跳过' % missing if missing else ''))
        else:
            log('稳定节点: 未写入任何文件(本轮结果行缺失), 保留上次 top_stable.* 文件')

    def _write_outputs(self, run_dir, top_v4, top_v6, merged, per_source_top, settings, log):
        """生成结果文件: IPv4 与 IPv6 分开输出

        IPv4: top_nodes.txt / top_nodes.yaml / all_sorted.txt(全量 v4 排序)
        IPv6: top_nodes_v6.txt / top_nodes_v6.yaml / all_sorted_v6.txt(全量 v6 排序)
        分源 Top 不分栈(按源分段, 官方 v6 源天然是纯 IPv6 段)。
        """
        node_cfg = settings.get('node', {})
        merged_v4 = [r for r in merged if ':' not in r['ip']]
        merged_v6 = [r for r in merged if ':' in r['ip']]

        def _write_txt(path, rows):
            with open(path, 'w', encoding='utf-8') as f:
                for r in rows:
                    f.write('%s#%s-%s-%s\n' % (r['ipport'], r['speed_text'],
                                               r['dc'] or 'CF', r['loc'] or 'XX'))

        # ---- IPv4 主输出 ----
        txt_path = os.path.join(run_dir, 'top_nodes.txt')
        _write_txt(txt_path, top_v4)
        yaml_path = os.path.join(run_dir, 'top_nodes.yaml')
        used_names = set()
        with open(yaml_path, 'w', encoding='utf-8') as f:
            f.write('proxies:\n')
            for r in top_v4:
                self._yaml_node(f, r, self._node_name(r, settings, used_names), node_cfg)
        all_path = os.path.join(run_dir, 'all_sorted.txt')
        _write_txt(all_path, merged_v4)

        # ---- IPv6 独立输出(v4/v6 分开订阅) ----
        v6_txt_path = os.path.join(run_dir, 'top_nodes_v6.txt')
        _write_txt(v6_txt_path, top_v6)
        v6_yaml_path = os.path.join(run_dir, 'top_nodes_v6.yaml')
        used_names_v6 = set()
        with open(v6_yaml_path, 'w', encoding='utf-8') as f:
            f.write('proxies:\n')
            for r in top_v6:
                self._yaml_node(f, r, self._node_name(r, settings, used_names_v6), node_cfg)
        v6_all_path = os.path.join(run_dir, 'all_sorted_v6.txt')
        _write_txt(v6_all_path, merged_v6)

        # 附加: 分源 Top N TXT (每个源单独提取速度最快的前 N 个节点, 纯数据+分组行)
        per_source_n = max(1, int(settings.get('per_source_top_n') or 5))
        by_source_path = os.path.join(run_dir, 'top_by_source.txt')
        with open(by_source_path, 'w', encoding='utf-8') as f:
            for g in (per_source_top or []):
                f.write('===== %s (%d) =====\n' % (g['name'], len(g['nodes'])))
                for r in g['nodes']:
                    f.write('%s#%s-%s-%s\n' % (
                        r['ipport'], r['speed_text'], r['dc'] or 'CF', r['loc'] or 'XX'))
                f.write('\n')

        # 附加: 分源 Top N 的 Clash YAML (节点名带 [源名] 前缀以区分来源)
        used_by = set()
        by_source_yaml_path = os.path.join(run_dir, 'top_by_source.yaml')
        with open(by_source_yaml_path, 'w', encoding='utf-8') as f:
            f.write('proxies:\n')
            for g in (per_source_top or []):
                for r in g['nodes']:
                    base = self._node_name(r, settings, set())
                    name = unique_name('[%s] %s' % (g['name'], base), used_by)
                    self._yaml_node(f, r, name, node_cfg)
        return {
            'txt': txt_path, 'yaml': yaml_path, 'all': all_path,
            'v6_txt': v6_txt_path, 'v6_yaml': v6_yaml_path, 'v6_all': v6_all_path,
            'by_source': by_source_path, 'by_source_yaml': by_source_yaml_path,
        }

    # ---- 质检子任务 ----
    def _run_qa(self, trigger):
        """复测 latest 的原始 Top 节点, 未达标节点从 top_nodes.txt/.yaml 剔除

        输入固定为 results/latest/qa_input.txt(完整任务提取的原始 Top 列表, 每次全量复测
        不随剔除缩小); 工作目录固定为 results/qa/ 只保留最近一次。
        CLI 的结果文件只包含达标节点(-nsbspeedmin 由 CLI 自行过滤), 因此未出现在结果
        文件中的节点视为本次测速未达标/失联, 一并剔除 —— 节点仍留在基准中, 下次质检
        达标后自动回归。与主任务共用执行锁; 只重写 latest 的两个 Top 文件并在 meta.json
        追加质检信息, 不触碰 runs 历史记录、历史节点池与主任务的合并/排序/提取逻辑。
        """
        run_id = 'qa_' + datetime.now().strftime('%Y%m%d_%H%M%S')
        t0 = time.time()  # 质检总耗时(结束时打印结果汇总用)
        # 同 _run(): 只清空内容, 序号继续递增
        self.log.clear()
        self._set(running=True, kind='qa', phase='qa', trigger=trigger, run_id=run_id,
                  started_at=now_str(), finished_at=None, current_source='质检',
                  progress_done=0, progress_total=1, message='质检进行中')
        self._reset_sub_progress()
        logf = None

        def log(msg):
            line = '[%s] %s' % (now_str('%H:%M:%S'), msg)
            self.log.write(line)
            if logf:
                logf.write(line + '\n')
                logf.flush()

        try:
            cfg = self.store.get()
            settings = cfg['settings']
            binary = self.locate_binary()
            if not binary:
                raise RuntimeError('未找到 cfdata 可执行文件, 请在「参数设置」中配置二进制路径')
            self.ensure_cfdata_config(binary)

            # 固定工作目录(每次覆盖, 只保留最近一次); 顺带清理旧版本的时间戳质检目录
            work_dir = QA_WORK_DIR
            os.makedirs(work_dir, exist_ok=True)
            try:
                for name in os.listdir(RESULTS_DIR):
                    if name != 'qa' and name.startswith('qa_') \
                            and os.path.isdir(os.path.join(RESULTS_DIR, name)):
                        shutil.rmtree(os.path.join(RESULTS_DIR, name), ignore_errors=True)
            except Exception:
                pass
            logf = open(os.path.join(work_dir, 'qa.log'), 'w', encoding='utf-8')
            log('===== 质检开始 (%s 触发, 运行 ID %s) ====='
                % ('定时' if trigger == 'cron' else '手动', run_id))

            # 订阅模板每日刷新(超过 24h 才重新拉取; 质检也重写订阅, 需要模板)
            if settings.get('sub_convert_enabled') and (settings.get('sub_convert_url') or '').strip():
                _, sub_msg = refresh_sub_template(settings)
                log(sub_msg)

            ipports_with_note = read_qa_input_lines()
            if not ipports_with_note:
                raise RuntimeError('最新结果中没有可质检的节点')
            ipports = [ipport for ipport, _ in ipports_with_note]
            try:
                speed_min = float(settings.get('speed_min') or 0)
            except (TypeError, ValueError):
                speed_min = 0.0

            input_name = 'qa_nodes.txt'
            with open(os.path.join(work_dir, input_name), 'w', encoding='utf-8') as f:
                for ipport in ipports:
                    f.write(ipport + '\n')
            log('质检输入: 原始 Top 节点 %d 个 (基准 latest/%s, 每次全量复测; '
                '剔除阈值 %.2f MB/s, 本地文件 %s)'
                % (len(ipports), QA_INPUT_NAME, speed_min, input_name))

            out_name = 'qa.csv'
            out_path = os.path.join(work_dir, out_name)
            cmd = self._build_cmd(binary, settings, out_name, input_name=input_name)
            rows, code, err = self._exec_cmd(cmd, work_dir, out_path, settings, log, '质检')
            if self._cancel:
                raise _CanceledError()
            if (code != 0) or err:
                raise RuntimeError('质检 CLI 执行失败 (退出码 %s, 结果 %d 行%s), latest 保持原样'
                                   % (code if code is not None else '异常', len(rows),
                                      ', ' + err if err else ''))
            # CLI 正常退出但没生成结果文件 = 异常运行, 保守放弃本次质检
            if not os.path.isfile(out_path):
                raise RuntimeError('质检 CLI 未生成结果文件, latest 保持原样')
            # 注: 结果文件允许 0 数据行 —— CLI 只输出达标节点, 全空说明本次全部未达标

            # 分类: 出现在结果文件中的 = 实测达标保留; 未出现的 = 本次测速未达标, 一并剔除。
            # cfdata 的 CSV 只输出达标节点(-nsbspeedmin 由 CLI 自行过滤), 因此"不在结果里"
            # 就是本次不达标/失联; 被剔除节点仍留在 qa_input 基准中, 下次达标自动回归(自愈)。
            by_key = {r['ipport']: r for r in rows}
            current_set = set(read_latest_top_ipports())  # 上次质检后的订阅内容
            kept, pruned, failed, revived = [], [], [], []
            for ipport, _note in ipports_with_note:
                r = by_key.get(ipport)
                if r is None:
                    failed.append(ipport)
                    pruned.append({'ipport': ipport, 'speed': '未出现在结果中'})
                    continue
                if r['speed'] is not None and r['speed'] >= speed_min:
                    kept.append(r)
                    if ipport not in current_set:
                        revived.append({'ipport': ipport, 'speed': r['speed_text']})
                else:
                    pruned.append({'ipport': ipport, 'speed': r['speed_text']})
            kept.sort(key=lambda r: (-r['speed'], r['latency_ms'] or 0, r['ipport']))
            final_rows = kept
            failed_set = set(failed)
            for p in pruned:
                if p['ipport'] in failed_set:
                    log('剔除: %s (未出现在质检结果中, 本次测速未达标或失联; '
                        '基准保留, 恢复达标后自动回归)' % p['ipport'])
                else:
                    log('剔除: %s (本次 %s, 低于 %.2f MB/s)' % (p['ipport'], p['speed'], speed_min))
            for v in revived:
                log('回归: %s (本次 %s, 速度已恢复达标, 重新写回订阅文件)' % (v['ipport'], v['speed']))

            # 只重写 latest 的 Top 文件(IPv4/IPv6 分开写; 分源/全量文件保留原样)
            kept_v4_n, kept_v6_n, sub_conv = self._rewrite_latest_top(final_rows, settings, log)

            prev_meta = load_latest_meta() or {}
            info = {
                'id': run_id, 'trigger': trigger, 'at': now_str(),
                'checked': len(ipports), 'kept': len(kept), 'file_count': len(final_rows),
                'kept_v4': kept_v4_n, 'kept_v6': kept_v6_n,
                'pruned_count': len(pruned), 'revived_count': len(revived),
                'pruned': [{'ipport': p['ipport'], 'speed': p['speed']} for p in pruned],
                'revived': [{'ipport': v['ipport'], 'speed': v['speed']} for v in revived],
                'source_run_id': prev_meta.get('run_id', ''),
                'sub_converted': sub_conv,
                'failed': failed,
            }
            update_latest_meta_qa(info)
            save_qa_record({'id': run_id, 'trigger': trigger, 'started_at': self.state.get('started_at'),
                            'finished_at': now_str(), 'status': 'success',
                            'checked': len(ipports), 'kept': len(kept),
                            'kept_v4': kept_v4_n, 'kept_v6': kept_v6_n,
                            'pruned_count': len(pruned), 'revived_count': len(revived),
                            'pruned': [p['ipport'] for p in pruned],
                            'revived': [v['ipport'] for v in revived],
                            'latest_run_id': prev_meta.get('run_id', ''), 'error': ''})
            log('质检完成: 检查 %d 个, 保留 %d 个(IPv4 %d / IPv6 %d), 剔除 %d 个(其中 %d 个未出现在结果中), '
                '回归 %d 个 (latest 已重写, 运行历史/历史池未动)'
                % (len(ipports), len(kept), kept_v4_n, kept_v6_n,
                   len(pruned), len(failed), len(revived)))
            # 把质检结果清单打印到日志(保留节点与刚重写的订阅文件一致)
            self._log_qa_summary(log, run_id, trigger, time.time() - t0, len(ipports),
                                 final_rows, pruned, revived, failed)
            if not final_rows:
                log('警告: Top 节点本次全部未达标, latest 订阅内容为空, 建议尽快触发一次完整任务')
            log('===== 质检完成 =====')
            self._set(phase='done', running=False, finished_at=now_str(),
                      message='质检完成: 保留 %d (v4 %d / v6 %d) / 剔除 %d / 回归 %d'
                              % (len(final_rows), kept_v4_n, kept_v6_n,
                                 len(pruned), len(revived)))
        except _CanceledError:
            msg = '质检已取消, latest 保持原样'
            log(msg)
            save_qa_record({'id': run_id, 'trigger': trigger, 'started_at': self.state.get('started_at'),
                            'finished_at': now_str(), 'status': 'canceled',
                            'checked': 0, 'kept': 0, 'pruned_count': 0, 'pruned': [],
                            'latest_run_id': '', 'error': msg})
            self._set(phase='canceled', running=False, finished_at=now_str(), message=msg)
        except Exception as e:
            msg = '质检失败: %s' % e
            log(msg)
            save_qa_record({'id': run_id, 'trigger': trigger, 'started_at': self.state.get('started_at'),
                            'finished_at': now_str(), 'status': 'error',
                            'checked': 0, 'kept': 0, 'pruned_count': 0, 'pruned': [],
                            'latest_run_id': '', 'error': str(e)})
            self._set(phase='error', running=False, finished_at=now_str(), message=msg)
        finally:
            if logf:
                logf.close()
            with self._lock:
                self._running = False
                self._proc = None

    def _rewrite_latest_top(self, kept, settings, log):
        """用质检后的保留节点重写 results/latest/ 的 Top 文件(IPv4 / IPv6 分开)

        kept: 全部保留节点(混合双栈); 写入时按地址族拆分 ——
        IPv4 → top_nodes.txt / top_nodes.yaml, IPv6 → top_nodes_v6.txt / top_nodes_v6.yaml
        启用订阅模板转换时, 两个 YAML 直接重写为完整 Clash 订阅(节点剔除后同步生效),
        否则保持片段格式。
        """
        node_cfg = settings.get('node', {})
        sub_tpl = load_sub_template_text() if settings.get('sub_convert_enabled') else None
        kept_v4 = [r for r in kept if ':' not in r['ip']]
        kept_v6 = [r for r in kept if ':' in r['ip']]
        converted = []

        def _write_pair(rows, txt_name, yaml_name):
            txt_path = os.path.join(LATEST_DIR, txt_name)
            yaml_path = os.path.join(LATEST_DIR, yaml_name)
            tmp = txt_path + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                for r in rows:
                    f.write('%s#%s-%s-%s\n' % (r['ipport'], r['speed_text'],
                                               r['dc'] or 'CF', r['loc'] or 'XX'))
            os.replace(tmp, txt_path)
            used_names = set()
            names = [self._node_name(r, settings, used_names) for r in rows]
            if sub_tpl and names:
                try:
                    entries = [(name, r['ip'], r['port'])
                               for name, r in zip(names, rows)]
                    text = render_clash_subscription(sub_tpl, entries)
                    tmp = yaml_path + '.tmp'
                    with open(tmp, 'w', encoding='utf-8') as f:
                        f.write(text)
                    os.replace(tmp, yaml_path)
                    converted.append(yaml_name)
                    return
                except Exception as e:
                    log('latest/%s 订阅转换失败 (%s), 本轮回退片段格式' % (yaml_name, e))
            tmp = yaml_path + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                f.write('proxies:\n')
                for name, r in zip(names, rows):
                    self._yaml_node(f, r, name, node_cfg)
            os.replace(tmp, yaml_path)

        try:
            os.makedirs(LATEST_DIR, exist_ok=True)
            _write_pair(kept_v4, 'top_nodes.txt', 'top_nodes.yaml')
            _write_pair(kept_v6, 'top_nodes_v6.txt', 'top_nodes_v6.yaml')
            log('已重写 latest: top_nodes.txt / top_nodes.yaml (IPv4 %d 个) + '
                'top_nodes_v6.txt / top_nodes_v6.yaml (IPv6 %d 个), '
                '节点速度已刷新为本次质检实测值%s'
                % (len(kept_v4), len(kept_v6),
                   ', YAML 为完整 Clash 订阅(模板转换)' if sub_tpl else ''))
            return len(kept_v4), len(kept_v6), converted
        except Exception as e:
            log('重写 latest 失败: %s' % e)
            raise


class _CanceledError(Exception):
    pass


# ---------------------------------------------------------------- 运行记录
def _runs_lock():
    global _RUNS_LOCK
    try:
        return _RUNS_LOCK
    except NameError:
        _RUNS_LOCK = threading.RLock()
        return _RUNS_LOCK


def load_runs():
    with _runs_lock():
        if not os.path.exists(RUNS_INDEX_PATH):
            return []
        try:
            with open(RUNS_INDEX_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []


def save_run_record(record):
    with _runs_lock():
        os.makedirs(RESULTS_DIR, exist_ok=True)
        runs = load_runs()
        runs = [r for r in runs if r.get('id') != record['id']]
        runs.append(record)
        runs.sort(key=lambda r: r.get('id', ''), reverse=True)
        runs = runs[:100]  # 只保留最近 100 次
        tmp = RUNS_INDEX_PATH + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(runs, f, ensure_ascii=False, indent=2)
        os.replace(tmp, RUNS_INDEX_PATH)


def load_run_record(run_id):
    for r in load_runs():
        if r.get('id') == run_id:
            return r
    return None


def load_latest_meta():
    """读取 results/latest/meta.json, 附带各文件是否存在; 无最新结果时返回 None"""
    meta_path = os.path.join(LATEST_DIR, 'meta.json')
    if not os.path.isfile(meta_path):
        return None
    try:
        with open(meta_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    exists = {}
    for key, fname in (data.get('files') or {}).items():
        exists[key] = os.path.isfile(os.path.join(LATEST_DIR, fname))
    data['exists'] = exists
    return data


# ---------------------------------------------------------------- 质检记录
_QA_LOCK = threading.RLock()


def load_qa_runs():
    """读取质检历史记录(最近 50 条, 按时间倒序)"""
    with _QA_LOCK:
        if not os.path.exists(QA_RUNS_PATH):
            return []
        try:
            with open(QA_RUNS_PATH, 'r', encoding='utf-8') as f:
                runs = json.load(f)
            if isinstance(runs, list):
                return [r for r in runs if isinstance(r, dict)]
        except Exception:
            pass
        return []


def save_qa_record(rec):
    with _QA_LOCK:
        runs = [r for r in load_qa_runs() if r.get('id') != rec.get('id')]
        runs.append(rec)
        runs.sort(key=lambda r: r.get('id', ''), reverse=True)
        runs = runs[:50]
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            tmp = QA_RUNS_PATH + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(runs, f, ensure_ascii=False, indent=2)
            os.replace(tmp, QA_RUNS_PATH)
        except Exception as e:
            sys.stderr.write('保存质检记录失败: %s\n' % e)


def _read_top_lines(path):
    """读取 top_nodes 格式文件的 (ip:port, 注释) 列表

    注释为 # 后的 "速度-数据中心-位置", 用于未测到节点原样保留时还原行内容。
    """
    if not os.path.isfile(path):
        return []
    out, seen = [], set()
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                ipport, _, note = line.partition('#')
                ipport = ipport.strip()
                if ipport and ':' in ipport and ipport not in seen:
                    seen.add(ipport)
                    out.append((ipport, note.strip()))
    except Exception:
        return []
    return out


def read_latest_top_lines():
    """读取 results/latest/top_nodes.txt(IPv4) 与 top_nodes_v6.txt(IPv6) 合并的
    (ip:port, 注释) 列表 —— 双栈分开输出后, 订阅内容为两文件之和"""
    out, seen = [], set()
    for name in ('top_nodes.txt', 'top_nodes_v6.txt'):
        for ipport, note in _read_top_lines(os.path.join(LATEST_DIR, name)):
            if ipport not in seen:
                seen.add(ipport)
                out.append((ipport, note))
    return out


def read_latest_top_ipports():
    """读取 results/latest/top_nodes.txt 中的 ip:port 列表"""
    return [ipport for ipport, _ in read_latest_top_lines()]


def read_qa_input_lines():
    """质检输入基准: results/latest/qa_input.txt(完整任务提取的原始 Top 节点列表)

    每次质检都从这份固定基准全量复测, 不随剔除缩小 —— 被剔除节点速度恢复后自动回归。
    基准不存在时(旧版本数据/首次), 用当前 top_nodes.txt 初始化并落盘。
    """
    lines = _read_top_lines(QA_INPUT_PATH)
    if lines:
        return lines
    lines = read_latest_top_lines()
    if lines:
        try:
            os.makedirs(LATEST_DIR, exist_ok=True)
            with open(QA_INPUT_PATH, 'w', encoding='utf-8') as f:
                for ipport, note in lines:
                    f.write('%s#%s\n' % (ipport, note) if note else ipport + '\n')
        except Exception as e:
            sys.stderr.write('初始化质检基准失败: %s\n' % e)
    return lines


def update_latest_meta_qa(info):
    """在 latest/meta.json 追加质检信息; top_count/top_v6_count 同步为重写后
    两套文件中的实际节点数(IPv4 / IPv6 分开)"""
    meta_path = os.path.join(LATEST_DIR, 'meta.json')
    try:
        data = {}
        if os.path.isfile(meta_path):
            try:
                with open(meta_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    data = {}
            except Exception:
                data = {}
        data['qa'] = info
        # kept_v4/kept_v6 = 重写后两套文件中的实际节点数(本次实测达标保留)
        data['top_count'] = info.get('kept_v4', info.get('kept', 0))
        data['top_v6_count'] = info.get('kept_v6', 0)
        # 订阅转换标记同步为质检重写后的实际状态(空列表 = 回退片段格式)
        data['sub_converted'] = info.get('sub_converted') or []
        tmp = meta_path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, meta_path)
    except Exception as e:
        sys.stderr.write('更新 latest 质检信息失败: %s\n' % e)


# ---------------------------------------------------------------- 定时调度器
class CronScheduler(threading.Thread):
    def __init__(self, store: ConfigStore, runner: TaskRunner, interval=15):
        super().__init__(daemon=True, name='cron-scheduler')
        self.store = store
        self.runner = runner
        self.interval = interval
        self._last_fire = None
        self._last_fire_qa = None
        self._expr = None
        self._expr_str = None
        self._next_runs = []
        self._next_runs_at = 0.0

    def current_expr(self):
        cron = self.store.get().get('cron', {})
        if not cron.get('enabled'):
            return None
        try:
            return CronExpr(cron.get('expr', ''))
        except ValueError:
            return None

    def qa_current_expr(self):
        """质检独立定时的 cron 表达式; 未启用/无效时返回 None"""
        cron = self.store.get().get('qa_cron', {})
        if not cron.get('enabled'):
            return None
        try:
            return CronExpr(cron.get('expr', ''))
        except ValueError:
            return None

    def next_runs(self, count=3, force_refresh=False):
        expr = self.current_expr()
        if expr is None:
            return []
        if not force_refresh and self._next_runs and time.time() - self._next_runs_at < 60 \
                and self._expr_str == expr.expr:
            return self._next_runs[:count]
        try:
            self._next_runs = expr.next_runs(count=5)
            self._next_runs_at = time.time()
            self._expr_str = expr.expr
        except Exception:
            self._next_runs = []
        return self._next_runs[:count]

    def run(self):
        while True:
            try:
                expr = self.current_expr()
                if expr is not None:
                    now = datetime.now()
                    minute_key = now.strftime('%Y-%m-%d %H:%M')
                    if expr.matches(now) and self._last_fire != minute_key:
                        self._last_fire = minute_key
                        if self.runner.is_running():
                            self.runner.log.write('[%s] [cron] 定时触发时已有任务在运行, 本次跳过'
                                                  % now_str('%H:%M:%S'))
                        else:
                            self.runner.log.write('[%s] [cron] 定时任务触发' % now_str('%H:%M:%S'))
                            self.runner.start(trigger='cron')
                else:
                    self._last_fire = None
                # 质检独立定时: 仅复测 latest Top 节点, 与主任务共用执行锁(运行中则跳过)
                qa_expr = self.qa_current_expr()
                if qa_expr is not None:
                    now = datetime.now()
                    minute_key = now.strftime('%Y-%m-%d %H:%M')
                    if qa_expr.matches(now) and self._last_fire_qa != minute_key:
                        self._last_fire_qa = minute_key
                        if self.runner.is_running():
                            self.runner.log.write('[%s] [qa-cron] 定时质检触发时已有任务在运行, 本次跳过'
                                                  % now_str('%H:%M:%S'))
                        else:
                            self.runner.log.write('[%s] [qa-cron] 定时质检触发' % now_str('%H:%M:%S'))
                            self.runner.start_qa(trigger='cron')
                else:
                    self._last_fire_qa = None
            except Exception as e:
                sys.stderr.write('cron 调度异常: %s\n' % e)
            time.sleep(self.interval)


# ---------------------------------------------------------------- HTTP 服务
# 先确保数据目录存在(挂载点可能是空目录或未创建), 再迁移旧位置的状态文件,
# 最后加载配置(保证 ConfigStore 读到迁移后的文件)
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception as e:
    sys.stderr.write('数据目录创建失败(%s): %s\n' % (DATA_DIR, e))
MIGRATED_FILES = migrate_legacy_state()
STORE = ConfigStore()
HISTORY = HistoryPool()
RUNNER = TaskRunner(STORE)
SCHEDULER = CronScheduler(STORE, RUNNER)


def _cron_block(cron_cfg):
    """把 {enabled, expr} 配置渲染成前端可用的 cron 状态块(含下次运行时间)"""
    obj, err = None, ''
    try:
        obj = CronExpr(cron_cfg.get('expr', ''))
    except ValueError as e:
        err = str(e)
    next_runs = []
    if cron_cfg.get('enabled') and obj is not None:
        try:
            next_runs = [d.strftime('%Y-%m-%d %H:%M') for d in obj.next_runs(count=3)]
        except Exception:
            next_runs = []
    return {
        'enabled': bool(cron_cfg.get('enabled')),
        'expr': cron_cfg.get('expr', ''),
        'valid': obj is not None,
        'error': err,
        'description': obj.describe() if obj else '',
        'next_runs': next_runs,
    }


def api_state():
    cfg = STORE.get()
    cron = cfg.get('cron', {})
    snap = RUNNER.snapshot()
    cron_obj = None
    cron_err = ''
    try:
        cron_obj = CronExpr(cron.get('expr', ''))
    except ValueError as e:
        cron_err = str(e)
    next_runs = SCHEDULER.next_runs(3, force_refresh=True) if (cron.get('enabled') and cron_obj) else []
    runs = load_runs()
    last_run = None
    for r in runs:
        if r.get('status') in ('success', 'error', 'canceled'):
            last_run = {'id': r['id'], 'status': r['status'],
                        'finished_at': r.get('finished_at'),
                        'top_count': r.get('top_count', 0), 'total_nodes': r.get('total_nodes', 0)}
            break
    qa_records = load_qa_runs()
    qa_last = None
    if qa_records:
        q = qa_records[0]
        qa_last = {'id': q.get('id'), 'status': q.get('status'), 'at': q.get('finished_at'),
                   'trigger': q.get('trigger'), 'checked': q.get('checked', 0),
                   'kept': q.get('kept', 0), 'pruned_count': q.get('pruned_count', 0)}
    return {
        'running': snap['running'],
        'kind': snap.get('kind', ''),
        'phase': snap['phase'],
        'message': snap['message'],
        'current_source': snap['current_source'],
        'progress': {'done': snap['progress_done'], 'total': snap['progress_total']},
        'scan_progress': {'done': snap.get('scan_done', 0), 'total': snap.get('scan_total', 0)},
        'speed_progress': {'tested': snap.get('speed_tested', 0),
                           'total': snap.get('speed_total', 0),
                           'qualified': snap.get('speed_qualified', 0)},
        'trigger': snap['trigger'],
        'started_at': snap['started_at'],
        'cron': {
            'enabled': bool(cron.get('enabled')),
            'expr': cron.get('expr', ''),
            'valid': cron_obj is not None,
            'error': cron_err,
            'description': cron_obj.describe() if cron_obj else '',
            'next_runs': [d.strftime('%Y-%m-%d %H:%M') for d in next_runs],
        },
        'qa_cron': _cron_block(cfg.get('qa_cron', {})),
        'qa_last': qa_last,
        'sources_enabled': len(STORE.enabled_sources()),
        'sources_total': len(cfg.get('sources', [])),
        'last_run': last_run,
        'now': now_str(),
    }


class Handler(BaseHTTPRequestHandler):
    server_version = 'CFDataWeb/1.0'

    def log_message(self, fmt, *args):
        pass  # 静默访问日志

    # ---- 访问控制(可选, 默认关闭) ----
    def _authorized(self):
        """Basic 认证校验; 未启用时直接放行"""
        if not AUTH_ENABLED:
            return True
        header = self.headers.get('Authorization') or ''
        if not header.startswith('Basic '):
            return False
        try:
            raw = base64.b64decode(header[6:]).decode('utf-8')
        except Exception:
            return False
        user, sep, pwd = raw.partition(':')
        if not sep:
            return False
        # 定长比较, 避免时序侧信道
        return (hmac.compare_digest(user, AUTH_USER)
                and hmac.compare_digest(pwd, AUTH_PASS))

    @staticmethod
    def _token_ok(qs):
        """下载接口的订阅令牌校验: 未配置 CFDATA_SUB_TOKEN 时不可用"""
        if not SUB_TOKEN:
            return False
        token = (qs.get('token') or [''])[0]
        return bool(token) and hmac.compare_digest(token, SUB_TOKEN)

    def _require_auth(self, qs=None):
        """未通过鉴权时返回 401 并结束请求; qs 非空且令牌匹配则放行"""
        if self._authorized():
            return True
        if qs is not None and self._token_ok(qs):
            return True
        body = b'Unauthorized'
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm="cfdata-web-manager"')
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        return False

    # ---- 响应辅助 ----
    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        length = int(self.headers.get('Content-Length') or 0)
        if length <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode('utf-8'))
        except Exception:
            return {}

    def _file(self, path, download_name=None, mime='application/octet-stream'):
        if not os.path.isfile(path):
            self._json({'ok': False, 'error': '文件不存在'}, 404)
            return
        with open(path, 'rb') as f:
            body = f.read()
        self.send_response(200)
        self.send_header('Content-Type', mime + ('; charset=utf-8' if mime.startswith('text/') else ''))
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Content-Disposition',
                         'attachment; filename="%s"' % (download_name or os.path.basename(path)))
        self.end_headers()
        self.wfile.write(body)

    # ---- 路由 ----
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        try:
            # 启用鉴权时, 仅订阅下载路径接受 ?token= 免密码访问(便于 Clash 等客户端拉取)
            if not self._require_auth(qs if path.startswith('/api/download/') else None):
                return
            if path in ('/', '/index.html'):
                index = os.path.join(WEB_DIR, 'index.html')
                if not os.path.exists(index):
                    self._json({'ok': False, 'error': 'web/index.html 缺失'}, 500)
                    return
                with open(index, 'rb') as f:
                    body = f.read()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
                self.send_header('Pragma', 'no-cache')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path == '/api/state':
                self._json({'ok': True, 'data': api_state()})
                return

            if path == '/api/sources':
                self._json({'ok': True, 'data': STORE.get().get('sources', [])})
                return

            if path == '/api/history':
                self._json({'ok': True, 'data': {
                    'nodes': HISTORY.list_nodes(),
                    'stats': HISTORY.stats(),
                    'enabled': bool(STORE.get().get('settings', {}).get('history_test_enabled', True)),
                }})
                return

            if path == '/api/settings':
                self._json({'ok': True, 'data': STORE.get().get('settings', {})})
                return

            if path == '/api/status':
                after = int((qs.get('after') or ['0'])[0])
                lines = RUNNER.log.read(after)
                snap = RUNNER.snapshot()
                sp_total = snap.get('speed_total', 0)
                sp_tested = snap.get('speed_tested', 0)
                speed_pct = round(sp_tested / sp_total * 100, 1) if sp_total > 0 else None
                self._json({'ok': True, 'data': {
                    'seq': lines[-1][0] if lines else after,
                    'lines': [l for _, l in lines],
                    'running': snap['running'],
                    'phase': snap['phase'],
                    'current_source': snap['current_source'],
                    'progress': {'done': snap['progress_done'], 'total': snap['progress_total']},
                    'scan_progress': {'done': snap.get('scan_done', 0), 'total': snap.get('scan_total', 0)},
                    'speed_progress': {'tested': sp_tested, 'total': sp_total,
                                       'qualified': snap.get('speed_qualified', 0),
                                       'percent': speed_pct},
                }})
                return

            if path == '/api/runs':
                runs = load_runs()
                slim = [{k: r.get(k) for k in
                         ('id', 'trigger', 'started_at', 'finished_at', 'status',
                          'total_nodes', 'top_count', 'top_v6_count', 'files')} for r in runs]
                self._json({'ok': True, 'data': slim})
                return

            if path == '/api/latest':
                data = load_latest_meta()
                self._json({'ok': True, 'data': data})
                return

            if path == '/api/qa':
                self._json({'ok': True, 'data': {
                    'records': load_qa_runs()[:20],
                    'cron': _cron_block(STORE.get().get('qa_cron', {})),
                }})
                return

            if path == '/api/sub_template':
                self._json({'ok': True, 'data': sub_template_status()})
                return

            m = re.fullmatch(r'/api/runs/([\w.:-]+)', path)
            if m:
                rec = load_run_record(m.group(1))
                if not rec:
                    self._json({'ok': False, 'error': '运行记录不存在'}, 404)
                    return
                # 附带文件内容预览
                run_dir = os.path.join(RESULTS_DIR, m.group(1))
                for key, fname in (rec.get('files') or {}).items():
                    fp = os.path.join(run_dir, os.path.basename(fname))
                    if os.path.isfile(fp) and key in ('txt', 'yaml', 'v6_txt', 'v6_yaml'):
                        try:
                            with open(fp, 'r', encoding='utf-8') as f:
                                rec.setdefault('preview', {})[key] = f.read(20000)
                        except Exception:
                            pass
                self._json({'ok': True, 'data': rec})
                return

            m = re.fullmatch(r'/api/download/([\w.:-]+)/([\w.-]+)', path)
            if m:
                run_id, fname = m.group(1), m.group(2)
                run_dir = os.path.join(RESULTS_DIR, run_id)
                fp = os.path.join(run_dir, os.path.basename(fname))  # 防目录穿越
                if not os.path.abspath(fp).startswith(os.path.abspath(run_dir)):
                    self._json({'ok': False, 'error': '非法路径'}, 400)
                    return
                mime = 'text/yaml' if fname.endswith(('.yaml', '.yml')) else 'text/plain'
                # latest 固定路径保持原始文件名, 便于订阅工具按名保存
                dl_name = fname if run_id == 'latest' else '%s_%s' % (run_id, fname)
                self._file(fp, download_name=dl_name, mime=mime)
                return

            if path == '/api/binary':
                b = RUNNER.locate_binary()
                self._json({'ok': True, 'data': {'path': b or '', 'found': b is not None}})
                return

            self._json({'ok': False, 'error': '接口不存在'}, 404)
        except BrokenPipeError:
            pass
        except Exception as e:
            self._json({'ok': False, 'error': '服务器错误: %s' % e}, 500)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        body = self._body()  # 先消费请求体, 避免 401 时残留数据污染后续请求
        try:
            if not self._require_auth():  # 写操作一律需要认证(令牌仅用于下载)
                return
            if path == '/api/sources':
                try:
                    src = STORE.add_source(body.get('name', ''), body.get('url', ''),
                                           bool(body.get('enabled', True)))
                except ValueError as e:
                    return self._json({'ok': False, 'error': str(e)}, 400)
                return self._json({'ok': True, 'data': src})
            m = re.fullmatch(r'/api/sources/([\w.:-]+)', path)
            if m:
                try:
                    src = STORE.update_source(m.group(1), body)
                    return self._json({'ok': True, 'data': src})
                except KeyError:
                    return self._json({'ok': False, 'error': '源不存在'}, 404)
                except ValueError as e:
                    return self._json({'ok': False, 'error': str(e)}, 400)

            if path == '/api/settings':
                patch = {'settings': body}
                STORE.update(patch)
                return self._json({'ok': True, 'data': STORE.get().get('settings', {})})

            if path == '/api/cron':
                expr = (body.get('expr') or '').strip()
                enabled = bool(body.get('enabled'))
                if enabled:
                    try:
                        CronExpr(expr)
                    except ValueError as e:
                        return self._json({'ok': False, 'error': 'cron 表达式无效: %s' % e}, 400)
                STORE.update({'cron': {'enabled': enabled, 'expr': expr}})
                SCHEDULER.next_runs(3, force_refresh=True)
                return self._json({'ok': True, 'data': api_state()['cron']})

            if path == '/api/run':
                ok, msg = RUNNER.start(trigger='manual')
                return self._json({'ok': ok, 'message': msg}, 200 if ok else 409)

            if path == '/api/qa':
                ok, msg = RUNNER.start_qa(trigger='manual')
                return self._json({'ok': ok, 'message': msg}, 200 if ok else 409)

            if path == '/api/sub_template/refresh':
                # 支持携带页面表单值: 填好模板设置后直接点刷新即可生效,
                # 无需先点「保存设置」(收到的值先持久化, 再用最新配置刷新)
                patch = {}
                if 'sub_convert_url' in body:
                    patch['sub_convert_url'] = str(body.get('sub_convert_url') or '').strip()
                if 'sub_convert_enabled' in body:
                    patch['sub_convert_enabled'] = bool(body.get('sub_convert_enabled'))
                if patch:
                    STORE.update({'settings': patch})
                cfg = STORE.get()
                _, msg = refresh_sub_template(cfg['settings'], force=True, ignore_enabled=True)
                ok = os.path.isfile(SUB_TEMPLATE_PATH)
                return self._json({'ok': ok, 'message': msg,
                                   'data': sub_template_status()}, 200 if ok else 400)

            if path == '/api/qa_cron':
                expr = (body.get('expr') or '').strip()
                enabled = bool(body.get('enabled'))
                if enabled:
                    try:
                        CronExpr(expr)
                    except ValueError as e:
                        return self._json({'ok': False, 'error': 'cron 表达式无效: %s' % e}, 400)
                STORE.update({'qa_cron': {'enabled': enabled, 'expr': expr}})
                return self._json({'ok': True, 'data': _cron_block(STORE.get().get('qa_cron', {}))})

            if path == '/api/cancel':
                ok, msg = RUNNER.cancel()
                return self._json({'ok': ok, 'message': msg}, 200 if ok else 409)

            return self._json({'ok': False, 'error': '接口不存在'}, 404)
        except BrokenPipeError:
            pass
        except ValueError as e:
            return self._json({'ok': False, 'error': str(e)}, 400)
        except Exception as e:
            return self._json({'ok': False, 'error': '服务器错误: %s' % e}, 500)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        # 删除操作(清历史池/删源)必须鉴权, 否则任何人都能清空你的节点池
        if not self._require_auth():
            return
        if parsed.path == '/api/history':
            HISTORY.clear()
            return self._json({'ok': True})
        m = re.fullmatch(r'/api/sources/([\w.:-]+)', parsed.path)
        if not m:
            return self._json({'ok': False, 'error': '接口不存在'}, 404)
        try:
            STORE.delete_source(m.group(1))
            return self._json({'ok': True})
        except KeyError:
            return self._json({'ok': False, 'error': '源不存在'}, 404)


def main():
    parser = argparse.ArgumentParser(description='CFData 优选 Web 管理平台')
    parser.add_argument('--host', default='0.0.0.0', help='监听地址 (默认 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8088, help='监听端口 (默认 8088)')
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(WEB_DIR, exist_ok=True)

    # 旧版本把配置放在应用目录(容器内 /app), 迁移到数据目录以支持 Docker 挂载持久化
    if MIGRATED_FILES:
        print('[init] 已迁移旧配置到数据目录: %s (%s)' % (DATA_DIR, ', '.join(MIGRATED_FILES)))

    print('[init] 配置文件: %s (%s)' % (
        CONFIG_PATH, '已存在' if os.path.exists(CONFIG_PATH) else '将使用默认配置'))
    print('[init] 数据目录: %s%s' % (
        DATA_DIR, '' if DATA_DIR == APP_DIR else ' (已持久化, 重启不丢失)'))
    print('[init] 历史节点池: %d 个节点 (%s)' % (
        len(HISTORY), '复测已开启' if STORE.get()['settings'].get('history_test_enabled', True)
        else '复测已关闭'))
    qa_cron = STORE.get().get('qa_cron', {})
    print('[init] Top 节点质检: %s' % (
        '定时已启用 (%s)' % qa_cron.get('expr', '') if qa_cron.get('enabled') else '手动触发(可配置独立定时)'))
    if AUTH_ENABLED:
        print('[init] 访问控制: Basic 认证已启用 (用户 %s)%s'
              % (AUTH_USER, ', 订阅令牌已配置' if SUB_TOKEN else ', 未配置订阅令牌'))
    else:
        print('[init] 访问控制: 未启用(设置 CFDATA_AUTH_USER/PASS 开启) —— '
              '请勿将端口直接暴露到公网')

    # 启动时预生成 cfdata 配置文件, 避免首次正式任务被中断
    binary = RUNNER.locate_binary()
    if binary:
        RUNNER.ensure_cfdata_config(binary)
        print('[init] cfdata 二进制: %s' % binary)
        print('[init] cfdata 配置: %s (%s)' % (
            CFDATA_CONFIG_PATH, '已存在' if os.path.exists(CFDATA_CONFIG_PATH) else '生成失败, 首次任务时会自动重试'))
    else:
        print('[init] 警告: 未找到 cfdata 可执行文件, 请在 Web 界面「参数设置」中配置路径')

    SCHEDULER.start()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.daemon_threads = True
    print('=' * 56)
    print('  CFData 优选 Web 管理平台 已启动')
    print('  访问地址: http://%s:%d/' % (
        args.host if args.host not in ('0.0.0.0', '::') else '127.0.0.1', args.port))
    print('  工作目录: %s' % APP_DIR)
    print('=' * 56)

    def shutdown(sig, frame):
        print('\n正在停止服务...')
        try:
            RUNNER.cancel()
        except Exception:
            pass
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print('服务已停止')


if __name__ == '__main__':
    main()
