#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CFData 优选 Web 管理平台 —— 核心逻辑自测

纯标准库 unittest, 无需任何第三方依赖:  python3 tests/test_core.py
(或 python3 -m unittest discover -s tests)

导入 app 模块会在模块级创建 STORE/HISTORY/RUNNER(调度线程仅在 main() 里启动),
因此测试前先把 CFDATA_DATA_DIR 指向临时目录, 避免污染真实数据。
"""

import base64
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import unittest

_TMP = tempfile.mkdtemp(prefix='cfdata-test-')
os.environ['CFDATA_DATA_DIR'] = _TMP

_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'app.py')
_spec = importlib.util.spec_from_file_location('cfdata_app', _APP)
app = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(app)


def _row(ipport, speed_text='10.00MB/s', latency='30ms', dc='HKG', loc='HK'):
    ip, _, port = ipport.rpartition(':')
    return {'ip': ip, 'port': port, 'ipport': ipport,
            'speed': app.parse_speed_mb(speed_text) or -1.0,
            'speed_text': speed_text, 'latency': latency,
            'latency_ms': app.parse_latency_ms(latency),
            'dc': dc, 'loc': loc, 'region': '', 'city': ''}


class TestIpportFormat(unittest.TestCase):
    """ip:port 规范化: IPv4 原样, 裸 IPv6 补方括号, 非法输入不改写"""

    def test_ipv4_unchanged(self):
        for v in ('1.2.3.4:443', '104.16.132.229:2053'):
            self.assertEqual(app.bracket_v6_ipport(v), v)

    def test_bracketed_v6_unchanged(self):
        self.assertEqual(app.bracket_v6_ipport('[2606:4700::1]:443'),
                         '[2606:4700::1]:443')

    def test_bare_v6_gets_bracket(self):
        self.assertEqual(app.bracket_v6_ipport('2606:4700::1:443'),
                         '[2606:4700::1]:443')
        self.assertEqual(app.bracket_v6_ipport('::1:443'), '[::1]:443')

    def test_no_port_not_mangled(self):
        """回归: 不带端口的裸地址曾被误加括号 -> [2606:4700:]:1"""
        for v in ('2606:4700::1', '2606:4700::', '2606:4700::1:0',
                  '2606:4700::1:99999'):
            self.assertEqual(app.bracket_v6_ipport(v), v, v)

    def test_empty(self):
        self.assertEqual(app.bracket_v6_ipport(''), '')
        self.assertIsNone(app.bracket_v6_ipport(None))


class TestParsers(unittest.TestCase):
    def test_speed(self):
        self.assertAlmostEqual(app.parse_speed_mb('42.41MB/s'), 42.41)
        self.assertIsNone(app.parse_speed_mb('未测速'))
        self.assertIsNone(app.parse_speed_mb(''))

    def test_latency(self):
        self.assertAlmostEqual(app.parse_latency_ms('45.2ms'), 45.2)
        self.assertEqual(app.parse_latency_ms(''), float('inf'))

    def test_csv_parsing_normalizes_v6(self):
        d = tempfile.mkdtemp()
        p = os.path.join(d, 't.csv')
        with open(p, 'w', encoding='utf-8-sig', newline='') as f:
            f.write('ip:port,网络延迟,下载速度,数据中心,源IP位置,地区,城市\n')
            f.write('1.2.3.4:443,30ms,10.00MB/s,HKG,HK,,\n')
            f.write('2606:4700::1:443,40ms,8.00MB/s,LAX,US,,\n')
        rows = app.TaskRunner._parse_csv(p)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['ipport'], '1.2.3.4:443')
        self.assertEqual(rows[1]['ipport'], '[2606:4700::1]:443')
        shutil.rmtree(d, ignore_errors=True)


class TestCron(unittest.TestCase):
    def test_invalid(self):
        for bad in ('', '* * * *', '90 * * * *', 'a * * * *', '*/0 * * * *'):
            with self.assertRaises(ValueError):
                app.CronExpr(bad)

    def test_field_parsing(self):
        self.assertEqual(sorted(app.CronExpr('0 8 * * *').minute), [0])
        self.assertEqual(sorted(app.CronExpr('*/30 * * * *').minute), [0, 30])
        self.assertEqual(sorted(app.CronExpr('0 9 * * 1-5').dow), [1, 2, 3, 4, 5])
        self.assertEqual(sorted(app.CronExpr('0 8 * * 7').dow), [0])  # 7 == 周日

    def test_next_runs_in_future(self):
        from datetime import datetime
        runs = app.CronExpr('0 8 * * *').next_runs(3)
        self.assertEqual(len(runs), 3)
        self.assertTrue(all(r > datetime.now() for r in runs))
        self.assertTrue(runs[0] < runs[1] < runs[2])

    def test_describe(self):
        self.assertEqual(app.CronExpr('0 8 * * *').describe(), '每天 08:00')
        self.assertTrue(app.CronExpr('*/7 * * * *').describe().startswith('自定义'))


class TestLogBuffer(unittest.TestCase):
    def test_seq_never_rewinds(self):
        """回归: 每轮任务重建缓冲后 seq 回退会让前端游标失效, 日志框卡住"""
        b1 = app.LogBuffer()
        for i in range(50):
            b1.write('old %d' % i)
        last = b1.read(0)[-1][0]

        b1.clear()               # 模拟新一轮任务开始(不重建对象)
        b1.write('new 1')
        self.assertGreater(b1.read(0)[0][0], last)

    def test_new_buffer_keeps_global_counter(self):
        b1, b2 = app.LogBuffer(), app.LogBuffer()
        s1 = b1.write('a')
        s2 = b2.write('b')
        self.assertGreater(s2, s1)

    def test_maxlen(self):
        b = app.LogBuffer(maxlen=10)
        for i in range(50):
            b.write(str(i))
        self.assertEqual(len(b.read(0)), 10)


class TestHistoryPool(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), 'pool.json')
        self.pool = app.HistoryPool(self.path)

    def _mk(self, n, speed=10.0):
        return [_row('10.0.0.%d:443' % i, '%.2fMB/s' % speed) for i in range(n)]

    def test_add_and_capacity(self):
        self.pool.update_from_run('r1', [({'id': 's1', 'name': 'S1'}, self._mk(10))],
                                  None, False, {'speed_min': 5, 'history_pool_capacity': 5,
                                                'history_window_runs': 5,
                                                'history_evict_fails': 3})
        self.assertEqual(len(self.pool), 5)   # 容量上限挤出
        stats = self.pool.stats()
        self.assertEqual(stats['by_origin'].get('S1'), 5)

    def test_evict_by_consecutive_fails(self):
        self.pool.update_from_run('r1', [({'id': 's1', 'name': 'S1'}, self._mk(3))],
                                  None, False, {'speed_min': 5, 'history_pool_capacity': 250,
                                                'history_window_runs': 5,
                                                'history_evict_fails': 2})
        self.assertEqual(len(self.pool), 3)
        # 历史源执行成功但一个都没达标 -> 连续失败累计
        self.pool.update_from_run('r2', [], [], True,
                                  {'speed_min': 5, 'history_pool_capacity': 250,
                                   'history_window_runs': 5, 'history_evict_fails': 2})
        self.assertEqual(len(self.pool), 3)   # 第 1 次失败: fail_streak=1, 未淘汰
        self.pool.update_from_run('r3', [], [], True,
                                  {'speed_min': 5, 'history_pool_capacity': 250,
                                   'history_window_runs': 5, 'history_evict_fails': 2})
        self.assertEqual(len(self.pool), 0)   # 第 2 次失败 -> 淘汰

    def test_window_expiry(self):
        self.pool.update_from_run('r1', [({'id': 's1', 'name': 'S1'}, self._mk(2))],
                                  None, False, {'speed_min': 5, 'history_pool_capacity': 250,
                                                'history_window_runs': 1,
                                                'history_evict_fails': 3})
        self.assertEqual(len(self.pool), 2)
        # 新一轮没有这些节点 -> 超出滚动窗口, 过期清出
        self.pool.update_from_run('r2', [({'id': 's1', 'name': 'S1'},
                                          [_row('10.0.0.9:443')])],
                                  None, False, {'speed_min': 5, 'history_pool_capacity': 250,
                                                'history_window_runs': 1,
                                                'history_evict_fails': 3})
        self.assertEqual(len(self.pool), 1)

    def test_persistence(self):
        self.pool.update_from_run('r1', [({'id': 's1', 'name': 'S1'}, self._mk(4))],
                                  None, False, {'speed_min': 5, 'history_pool_capacity': 250,
                                                'history_window_runs': 5,
                                                'history_evict_fails': 3})
        reloaded = app.HistoryPool(self.path)
        self.assertEqual(len(reloaded), 4)


class TestConfigStore(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.orig = app.CONFIG_PATH
        app.CONFIG_PATH = os.path.join(self.dir, 'config.json')
        self.store = app.ConfigStore()

    def tearDown(self):
        app.CONFIG_PATH = self.orig
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_reject_bad_urls(self):
        for bad in ('', 'ftp://x/y', 'not-a-url', 'http://'):
            with self.assertRaises(ValueError):
                self.store.add_source('x', bad)

    def test_reject_duplicate(self):
        self.store.add_source('A', 'https://a.example/x.txt')
        with self.assertRaises(ValueError):
            self.store.add_source('B', 'https://a.example/x.txt')

    def test_deep_merge_keeps_defaults(self):
        self.store.update({'settings': {'top_n': 7}})
        s = self.store.get()['settings']
        self.assertEqual(s['top_n'], 7)
        self.assertEqual(s['speed_limit'], 9999)   # 未指定的默认值保留


class TestYamlHelpers(unittest.TestCase):
    def test_yaml_value(self):
        self.assertEqual(app.yaml_value(True), 'true')
        self.assertEqual(app.yaml_value(443), '443')
        self.assertEqual(app.yaml_value('HKG-HK'), 'HKG-HK')
        self.assertEqual(app.yaml_value('a b'), '"a b"')
        self.assertEqual(app.yaml_value(''), '""')

    def test_unique_name(self):
        used = set()
        self.assertEqual(app.unique_name('N', used), 'N')
        self.assertEqual(app.unique_name('N', used), 'N #2')
        self.assertEqual(app.unique_name('N', used), 'N #3')

    def test_name_template(self):
        out = app.render_name_template('{dc}-{loc}-{speed}MB/s',
                                       {'dc': 'HKG', 'loc': 'HK', 'speed': '42.41'})
        self.assertEqual(out, 'HKG-HK-42.41MB/s')
        self.assertEqual(app.render_name_template('{nope}-{dc}', {'dc': 'X'}), '-X')


class TestClashSubscription(unittest.TestCase):
    TPL = """port: 7890
proxies:
  - {name: "T1", server: 1.1.1.1, port: 443, type: trojan, password: p, sni: s.com, network: ws, ws-opts: {path: /, headers: {Host: s.com}}}
  - {name: "T2", server: 2.2.2.2, port: 443, type: trojan, password: p, sni: s.com, network: ws, ws-opts: {path: /, headers: {Host: s.com}}}
proxy-groups:
  - name: G1
    type: select
    proxies:
      - T1
      - T2
      - DIRECT
rules:
  - MATCH,G1
"""

    def test_nodes_replaced_and_groups_rewritten(self):
        out = app.render_clash_subscription(self.TPL, [('N1', '9.9.9.9', 443)])
        self.assertIn('name: N1', out)
        self.assertIn('server: 9.9.9.9', out)
        self.assertNotIn('1.1.1.1', out)
        self.assertIn('- N1', out)        # 策略组成员同步替换
        self.assertIn('- DIRECT', out)    # 非模板成员保留
        self.assertIn('MATCH,G1', out)    # 规则原文保留

    def test_empty_entries_rejected(self):
        with self.assertRaises(ValueError):
            app.render_clash_subscription(self.TPL, [])

    def test_bad_template_rejected(self):
        with self.assertRaises(ValueError):
            app.render_clash_subscription('port: 7890\n', [('N', '1.1.1.1', 443)])


class TestNodeNameLang(unittest.TestCase):
    """节点名中英文转换: name_lang=zh 把命中字段转中文, en 原样; 未知值保留"""

    def _row(self, dc='HKG', loc='Hong Kong', region='HK', city='Hong Kong'):
        return {'ip': '1.2.3.4', 'port': '443', 'ipport': '1.2.3.4:443',
                'speed': 42.41, 'speed_text': '42.41MB/s', 'latency': '10ms',
                'dc': dc, 'loc': loc, 'region': region, 'city': city}

    def _name(self, lang):
        runner = app.TaskRunner(app.STORE)
        settings = {'node': {'name_template': '{dc}-{loc}-{speed}MB/s',
                             'name_lang': lang}}
        return runner._node_name(self._row(), settings, set())

    def test_zh_converts_fields(self):
        self.assertEqual(self._name('zh'), '香港-香港-42.41MB/s')

    def test_en_keeps_original(self):
        self.assertEqual(self._name('en'), 'HKG-Hong Kong-42.41MB/s')

    def test_unknown_values_preserved(self):
        out = app.localize_node_fields({'dc': 'ZZZ', 'loc': 'Nowhere',
                                        'region': 'XX', 'city': 'Xyz'})
        self.assertEqual(out, {'dc': 'ZZZ', 'loc': 'Nowhere',
                               'region': 'XX', 'city': 'Xyz'})

    def test_region_code_and_city_name(self):
        out = app.localize_node_fields({'region': 'US', 'dc': 'LAX',
                                        'loc': 'Los Angeles', 'city': 'United States'})
        self.assertEqual(out['region'], '美国')
        self.assertEqual(out['dc'], '洛杉矶')
        self.assertEqual(out['loc'], '洛杉矶')
        self.assertEqual(out['city'], '美国')

    def test_localize_does_not_mutate_input(self):
        src = {'dc': 'HKG', 'loc': 'Hong Kong', 'region': 'HK', 'city': 'Hong Kong'}
        app.localize_node_fields(src)
        self.assertEqual(src['dc'], 'HKG')


class TestAuth(unittest.TestCase):
    """可选 Basic 认证: 未配置时放行, 配置后校验; 令牌仅用于下载路径"""

    class _FakeHandler(app.Handler):
        def __init__(self, headers):
            self.headers = headers

    def _h(self, header=None, user=None, pwd=None, token=None):
        h = {'Authorization': header} if header else {}
        return self._FakeHandler(h)

    def test_disabled_by_default(self):
        saved = (app.AUTH_ENABLED, app.AUTH_USER, app.AUTH_PASS, app.SUB_TOKEN)
        try:
            app.AUTH_ENABLED, app.SUB_TOKEN = False, ''
            self.assertTrue(self._h()._authorized())
            self.assertTrue(app.Handler._token_ok({'token': ['x']}) is False)
        finally:
            app.AUTH_ENABLED, app.AUTH_USER, app.AUTH_PASS, app.SUB_TOKEN = saved

    def test_basic_credentials(self):
        saved = (app.AUTH_ENABLED, app.AUTH_USER, app.AUTH_PASS)
        try:
            app.AUTH_ENABLED, app.AUTH_USER, app.AUTH_PASS = True, 'admin', 'pw123'
            good = 'Basic ' + base64.b64encode(b'admin:pw123').decode()
            bad = 'Basic ' + base64.b64encode(b'admin:bad').decode()
            self.assertTrue(self._h(header=good)._authorized())
            self.assertFalse(self._h(header=bad)._authorized())
            self.assertFalse(self._h(header='Basic !!!')._authorized())
            self.assertFalse(self._h()._authorized())  # 无 Authorization 头
        finally:
            app.AUTH_ENABLED, app.AUTH_USER, app.AUTH_PASS = saved

    def test_token_scope(self):
        saved = app.SUB_TOKEN
        try:
            app.SUB_TOKEN = 'tok'
            self.assertTrue(app.Handler._token_ok({'token': ['tok']}))
            self.assertFalse(app.Handler._token_ok({'token': ['nope']}))
            self.assertFalse(app.Handler._token_ok({}))
            # 令牌不能用于非下载路径: 由 do_GET 传入 qs 控制, 这里只校验匹配本身
        finally:
            app.SUB_TOKEN = saved


class TestProcTermination(unittest.TestCase):
    """子进程忽略 SIGTERM 时不得永久阻塞(否则执行锁无法释放)"""

    def test_kill_fallback(self):
        import time
        saved = app.PROC_TERM_WAIT
        app.PROC_TERM_WAIT = 2
        try:
            runner = app.TaskRunner(app.STORE)
            logs = []
            wd = tempfile.mkdtemp()
            cmd = ['bash', '-c', 'trap "" TERM; while true; do sleep 1; done']
            t0 = time.time()
            rows, code, err = runner._exec_cmd(
                cmd, wd, os.path.join(wd, 'x.csv'),
                {'timeout_minutes': 0.02, 'skip_geo_check': True},
                logs.append, 'test')
            self.assertLess(time.time() - t0, 25)   # 必须返回而不是挂起
            self.assertEqual(code, -9)              # SIGKILL
            self.assertFalse(runner.is_running())
            shutil.rmtree(wd, ignore_errors=True)
        finally:
            app.PROC_TERM_WAIT = saved


if __name__ == '__main__':
    ok = unittest.main(verbosity=2, exit=False).result.wasSuccessful()
    shutil.rmtree(_TMP, ignore_errors=True)
    sys.exit(0 if ok else 1)
