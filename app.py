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
import csv
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

APP_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(APP_DIR, 'config.json')
RESULTS_DIR = os.path.join(APP_DIR, 'results')
WEB_DIR = os.path.join(APP_DIR, 'web')
CFDATA_CONFIG_PATH = os.path.join(APP_DIR, 'cfdata-config.json')
RUNS_INDEX_PATH = os.path.join(RESULTS_DIR, 'runs.json')
# 固定的"最新结果"目录: 每次任务成功后同步覆盖, 路径不变便于外部订阅
LATEST_DIR = os.path.join(RESULTS_DIR, 'latest')
LATEST_FILES = ('top_nodes.txt', 'top_nodes.yaml', 'all_sorted.txt')

# CSV 导出字段(传给 cfdata -fields), 与中文表头一一对应
CSV_FIELDS = 'ipport,latency,speed,dc,loc,region,city'
HEADER_IPPORT = 'ip:port'
HEADER_LATENCY = '网络延迟'
HEADER_SPEED = '下载速度'
HEADER_DC = '数据中心'
HEADER_LOC = '源IP位置'
HEADER_REGION = '地区'
HEADER_CITY = '城市'

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
        'top_n': 20,                # 提取前 N 个节点
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
class LogBuffer:
    def __init__(self, maxlen=4000):
        self._lock = threading.Lock()
        self._lines = []       # [(seq, line)]
        self._seq = 0
        self._maxlen = maxlen

    def write(self, line):
        with self._lock:
            self._seq += 1
            self._lines.append((self._seq, line))
            if len(self._lines) > self._maxlen:
                self._lines = self._lines[-self._maxlen:]
            return self._seq

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
            'phase': 'idle',            # idle / preparing / running / finishing / done / error / canceled
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
        with self._lock:
            if self._running:
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
        self.log = LogBuffer()
        self._set(running=True, phase='preparing', trigger=trigger, run_id=run_id,
                  started_at=now_str(), finished_at=None, current_source='',
                  progress_done=0, progress_total=0, message='')
        logf = None
        try:
            cfg = self.store.get()
            settings = cfg['settings']
            sources = self.store.enabled_sources()
            if not sources:
                raise RuntimeError('没有已启用的 API 源, 请先在「API 源管理」中添加')
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
            log('已启用 API 源: %d 个 -> %s' % (len(sources), '、'.join(s['name'] for s in sources)))

            self._set(phase='running', progress_total=len(sources), progress_done=0)

            all_rows = {}
            source_reports = []
            for idx, src in enumerate(sources):
                if self._cancel:
                    raise _CanceledError()
                # 显示"第 idx+1 个源进行中", 完成后自然递增
                self._set(current_source=src['name'], progress_done=idx + 1)
                self._reset_sub_progress()
                label = '(%d/%d) %s' % (idx + 1, len(sources), src['name'])
                rows, report = self._run_one_source(binary, src, idx, run_dir, settings, log, label)
                source_reports.append(report)
                for r in rows:
                    key = r['ipport']
                    if key not in all_rows or r['speed'] > all_rows[key]['speed']:
                        all_rows[key] = r

            self._set(progress_done=len(sources), phase='finishing', current_source='汇总排序')
            merged = sorted(all_rows.values(),
                            key=lambda r: (-r['speed'], r['latency_ms'], r['ipport']))
            log('全部源测试完成: 共 %d 个有效节点 (已按速度降序排列)' % len(merged))

            top_n = int(settings.get('top_n') or 20)
            top = merged[:top_n]
            for i, r in enumerate(top):
                log('Top%-3d %s  速度=%s  延迟=%s  数据中心=%s' % (
                    i + 1, r['ipport'], r['speed_text'], r['latency'], r['dc'] or '-'))

            # ---- 生成两种格式 ----
            txt_path, yaml_path, all_path = self._write_outputs(run_dir, top, merged, settings, log)
            record = {
                'id': run_id,
                'trigger': trigger,
                'started_at': self.state['started_at'],
                'finished_at': now_str(),
                'status': 'success',
                'sources': source_reports,
                'total_nodes': len(merged),
                'top_count': len(top),
                'files': {
                    'txt': os.path.basename(txt_path),
                    'yaml': os.path.basename(yaml_path),
                    'all': os.path.basename(all_path),
                },
                'top_nodes': [self._node_payload(r, i, settings) for i, r in enumerate(top)],
            }
            save_run_record(record)
            self._sync_latest(run_dir, record, log)
            log('已保存 %d 个节点到两种格式: %s / %s' % (
                len(top), os.path.basename(txt_path), os.path.basename(yaml_path)))
            log('===== 任务完成 =====')
            self._set(phase='done', running=False, finished_at=now_str(),
                      current_source='', message='成功: %d 个节点' % len(top))
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
        out_name = 'source_%02d.csv' % (idx + 1)
        out_path = os.path.join(run_dir, out_name)
        if os.path.exists(out_path):
            os.remove(out_path)
        cmd = [
            binary,
            '-cli=true',
            '-mode=nsb',
            '-nsbsourceurl=' + src['url'],
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
        log('%s 开始测试: %s' % (label, src['url']))
        started = time.time()
        proc = None
        # 复用已缓存的 locations.json, 避免每次运行都重新下载数据中心位置信息
        cached_locations = os.path.join(APP_DIR, 'locations.json')
        if os.path.exists(cached_locations):
            try:
                shutil.copyfile(cached_locations, os.path.join(run_dir, 'locations.json'))
            except Exception:
                pass
        strict_geo = not bool(settings.get('skip_geo_check', True))
        try:
            # 严格模式下不给输入(交互确认将默认取消); 跳过模式下自动应答 y 作为兜底
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=run_dir,
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
            code = proc.wait()
            elapsed = time.time() - started
            if self._cancel:
                raise _CanceledError()
            if code != 0:
                log('%s 进程退出码 %d' % (label, code))
            rows = self._parse_csv(out_path)
            # 回写缓存 locations.json, 供后续运行复用(避免重复下载)
            run_locations = os.path.join(run_dir, 'locations.json')
            if os.path.exists(run_locations):
                try:
                    shutil.copyfile(run_locations, cached_locations)
                except Exception:
                    pass
            log('%s 完成: 耗时 %.1f 秒, 符合条件节点 %d 个' % (label, elapsed, len(rows)))
            return rows, {
                'name': src['name'], 'url': src['url'], 'ok': True,
                'count': len(rows), 'elapsed_sec': round(elapsed, 1),
                'exit_code': code,
            }
        except _CanceledError:
            raise
        except Exception as e:
            elapsed = time.time() - started
            log('%s 执行异常: %s' % (label, e))
            rows = self._parse_csv(out_path) if os.path.exists(out_path) else []
            return rows, {
                'name': src['name'], 'url': src['url'], 'ok': False,
                'count': len(rows), 'elapsed_sec': round(elapsed, 1), 'error': str(e),
            }
        finally:
            with self._lock:
                self._proc = None
            if proc is not None and proc.poll() is None:
                try:
                    proc.kill()
                except Exception:
                    pass

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

    def _sync_latest(self, run_dir, record, log):
        """将最新一次成功任务的结果同步到 results/latest/ (固定路径, 内容随每次任务更新)"""
        try:
            os.makedirs(LATEST_DIR, exist_ok=True)
            for fname in LATEST_FILES:
                src = os.path.join(run_dir, fname)
                if os.path.isfile(src):
                    shutil.copyfile(src, os.path.join(LATEST_DIR, fname))
            meta = {
                'run_id': record.get('id'),
                'finished_at': record.get('finished_at'),
                'trigger': record.get('trigger'),
                'top_count': record.get('top_count', 0),
                'total_nodes': record.get('total_nodes', 0),
                'files': {'txt': 'top_nodes.txt', 'yaml': 'top_nodes.yaml', 'all': 'all_sorted.txt'},
            }
            tmp = os.path.join(LATEST_DIR, 'meta.json.tmp')
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
            os.replace(tmp, os.path.join(LATEST_DIR, 'meta.json'))
            log('已同步最新结果到固定目录: results/latest/ '
                '(top_nodes.txt / top_nodes.yaml / all_sorted.txt)')
        except Exception as e:
            log('同步 latest 目录失败: %s' % e)

    def _write_outputs(self, run_dir, top, merged, settings, log):
        node_cfg = settings.get('node', {})
        used_names = set()

        # 格式 1: TXT  ip:port#速度-数据中心-位置
        txt_path = os.path.join(run_dir, 'top_nodes.txt')
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write('# CFData 优选 Top%d  生成时间: %s\n' % (len(top), now_str()))
            f.write('# 格式: ip:port#下载速度-数据中心-源IP位置\n')
            for r in top:
                f.write('%s#%s-%s-%s\n' % (r['ipport'], r['speed_text'], r['dc'] or 'CF', r['loc'] or 'XX'))

        # 格式 2: Clash YAML (与附件格式一致)
        yaml_path = os.path.join(run_dir, 'top_nodes.yaml')
        with open(yaml_path, 'w', encoding='utf-8') as f:
            f.write('# CFData 优选 Top%d 节点  生成时间: %s\n' % (len(top), now_str()))
            f.write('proxies:\n')
            for r in top:
                name = self._node_name(r, settings, used_names)
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

        # 附加: 全量排序结果
        all_path = os.path.join(run_dir, 'all_sorted.txt')
        with open(all_path, 'w', encoding='utf-8') as f:
            for r in merged:
                f.write('%s#%s-%s-%s\n' % (r['ipport'], r['speed_text'], r['dc'] or 'CF', r['loc'] or 'XX'))
        return txt_path, yaml_path, all_path


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


# ---------------------------------------------------------------- 定时调度器
class CronScheduler(threading.Thread):
    def __init__(self, store: ConfigStore, runner: TaskRunner, interval=15):
        super().__init__(daemon=True, name='cron-scheduler')
        self.store = store
        self.runner = runner
        self.interval = interval
        self._last_fire = None
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
            except Exception as e:
                sys.stderr.write('cron 调度异常: %s\n' % e)
            time.sleep(self.interval)


# ---------------------------------------------------------------- HTTP 服务
STORE = ConfigStore()
RUNNER = TaskRunner(STORE)
SCHEDULER = CronScheduler(STORE, RUNNER)


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
    return {
        'running': snap['running'],
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
        'sources_enabled': len(STORE.enabled_sources()),
        'sources_total': len(cfg.get('sources', [])),
        'last_run': last_run,
        'now': now_str(),
    }


class Handler(BaseHTTPRequestHandler):
    server_version = 'CFDataWeb/1.0'

    def log_message(self, fmt, *args):
        pass  # 静默访问日志

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
                          'total_nodes', 'top_count', 'files')} for r in runs]
                self._json({'ok': True, 'data': slim})
                return

            if path == '/api/latest':
                data = load_latest_meta()
                self._json({'ok': True, 'data': data})
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
                    if os.path.isfile(fp) and key in ('txt', 'yaml'):
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
        body = self._body()
        try:
            if path == '/api/sources':
                src = STORE.add_source(body.get('name', ''), body.get('url', ''),
                                       bool(body.get('enabled', True)))
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
