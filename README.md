# CFData 优选 Web 管理平台

基于 Python（纯标准库，零依赖）+ [CFData](https://github.com/PoemMisty/CFData-WEB) 的 Cloudflare 优选 IP 管理平台。

通过 Web 界面管理多个优选 API 源，自动顺序测速、按速度排序、提取 Top 节点，并保存为 **TXT** 与 **Clash YAML** 两种格式。

## 功能特性

- **多 API 源管理**：对应 `-nsbsourceurl` 参数，支持添加/删除/启用/禁用多个源，URL 重复校验
- **Cron 定时任务**：标准 5 段表达式（分 时 日 月 周），支持 `*` `,` `-` `/`，常用预设一键填充，未来 3 次运行时间预览
- **手动触发**：一键运行/取消，实时日志流（扫描进度、测速结果、Top 排名），防重复运行
- **自动排序提取**：所有源测完后按下载速度降序合并去重，提取前 N 个（默认 20）节点
- **双格式输出**：
  - `top_nodes.txt`：`ip:port#速度-数据中心-位置`
  - `top_nodes.yaml`：Clash trojan 节点（password/sni/ws-opts 可在界面配置）
  - 附带 `all_sorted.txt` 全量排序结果
- **历史记录**：每次运行保存各源测试情况、Top 节点表格、YAML 预览，支持在线下载

## 快速开始

```bash
# 1. 确保 cfdata-linux-amd64 与 app.py 在同一目录
chmod +x cfdata-linux-amd64

# 2. 启动（Python 3.8+，无需安装任何依赖）
python3 app.py --port 8088

# 3. 浏览器访问
# http://服务器IP:8088/
```

启动参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--host` | `0.0.0.0` | 监听地址 |
| `--port` | `8088` | 监听端口 |

## 使用流程

1. **API 源管理**：添加优选 API 地址（如 `https://bestcf.pages.dev/random-region/HK/all.txt`），可添加多个
2. **参数设置**：按需调整测速线程数（`-nsbspeedtest`）、最低速度（`-nsbspeedmin`）、提取节点数等
3. **定时任务**：配置 cron 表达式实现全自动定时优选（如 `0 8 * * *` 每天 08:00）
4. **手动触发**：点击「手动运行」立即测试，实时查看日志
5. **运行结果**：下载 `top_nodes.txt` / `top_nodes.yaml`，或查看 Top 节点详情

## 目录结构

```
cfdata_web/
├── app.py                 # 主程序（后端 + 静态页面服务）
├── cfdata-linux-amd64     # CFData 二进制（测速引擎）
├── web/
│   └── index.html         # Web 界面（单文件，无构建依赖）
├── config.json            # 运行时自动生成（源/定时/参数配置）
├── cfdata-config.json     # 运行时自动生成（cfdata CLI 配置）
└── results/               # 运行时自动生成
    ├── runs.json          # 运行记录索引
    └── 20260901_080000/   # 每次运行一个目录
        ├── run.log        # 运行日志
        ├── source_01.csv  # 各源原始结果
        ├── top_nodes.txt  # Top 节点（TXT 格式）
        ├── top_nodes.yaml # Top 节点（Clash YAML 格式）
        └── all_sorted.txt # 全量排序结果
```

## 输出格式示例

TXT（`top_nodes.txt`）：

```
104.16.132.229:443#42.41MB/s-HKG-HK
91.213.174.82:2053#32.91MB/s-HKG-HK
```

Clash YAML（`top_nodes.yaml`）：

```yaml
proxies:
  - name: HKG-HK-42.41MB/s
    server: 104.16.132.229
    port: 443
    type: trojan
    password: your-password
    sni: edgetunnel-ekw.pages.dev
    client-fingerprint: chrome
    skip-cert-verify: false
    network: ws
    ws-opts:
      path: /
      headers:
        Host: edgetunnel-ekw.pages.dev
```

## Docker 部署

镜像推送到 Docker Hub：`totootao/cfdata-web-manager`（GitHub Actions 自动构建，`main` 分支更新即重新推送）。

```bash
docker run -d \
  --name cfdata-web \
  --restart unless-stopped \
  -p 8088:8088 \
  -v cfdata-results:/app/results \
  -v cfdata-data:/app/data \
  totootao/cfdata-web-manager:latest
```

| 挂载卷 | 说明 |
| --- | --- |
| `/app/results` | 测试结果（TXT/YAML/日志/运行记录） |
| `/app/data` | 可选，持久化 `config.json` 等配置（配合 `-v cfdata-data:/app/data` 并设置环境变量时用于备份场景） |

本地构建镜像：

```bash
docker build -t cfdata-web-manager .
docker run -d -p 8088:8088 -v cfdata-results:/app/results cfdata-web-manager
```

> 镜像基于 `python:3.11-slim`，内置 cfdata-linux-amd64，仅支持 `linux/amd64` 平台。定时任务由应用内置 cron 调度器执行，容器需保持常驻运行。

## 等价命令

平台的任务执行等价于对每个已启用源运行：

```bash
./cfdata-linux-amd64 -cli -mode nsb \
  -nsbsourceurl "https://bestcf.pages.dev/random-region/HK/all.txt" \
  -nsbthreads 100 \
  -nsbspeedtest 5 \
  -nsbspeedmin 5 \
  -nsbspeedlimit 9999 \
  -nsbresultlimit 1000 \
  -nsbqualified=true \
  -nsbtls=true \
  -nsbiptype all \
  -skipgeo=true \
  -format csv \
  -fields ipport,latency,speed,dc,loc,region,city \
  -nsbout source_01.csv
```

> `-skipgeo`：cfdata 启动时会检测是否处于代理/VPN 环境并交互式询问是否继续；服务器/Docker 等无交互环境中该检测常因无法识别网络标签而取消任务，平台默认传 `-skipgeo=true` 跳过（可在「参数设置」中关闭恢复严格检测）。

## 注意事项

- cfdata 首次运行会生成 `cfdata-config.json`，平台启动时已自动预生成，无需手动处理
- `locations.json`（数据中心位置信息）首次运行后自动缓存复用，无需每次重新下载
- 测速线程数建议保持 5 以内，多 IP 并发会互相影响实际速度
- cfdata 原始输出的 `[speed] [x/9999 x%]` 中，9999 是 `-nsbspeedlimit` 上限（9999=不限）而非真实待测总数；平台已自动将该分母**重写为真实总数**（取自「开始测速：N 条记录」一行），日志显示为 `[speed] [已测/共N 百分比]`，与界面橙色测速进度条（已测 X / 共 N · 达标数）完全一致
- `-nsbspeedlimit` 设小（如 50）可在达标数量足够后提前结束测速，缩短任务时间
- 若 cfdata 未与本程序同目录，可在「参数设置」中指定二进制绝对路径
- 本项目仅供网络研究与学习用途

## 致谢

- [CFData-WEB](https://github.com/PoemMisty/CFData-WEB) — Cloudflare IP 测试与筛选工具
