# CFData 优选 Web 管理平台

基于 Python（纯标准库，零依赖）+ [CFData](https://github.com/PoemMisty/CFData-WEB) 的 Cloudflare 优选 IP 管理平台。

通过 Web 界面管理多个优选 API 源，自动顺序测速、按速度排序、提取 Top 节点，并保存为 **TXT** 与 **Clash YAML** 两种格式。

## 功能特性

- **多 API 源管理**：对应 `-nsbsourceurl` 参数，支持添加/删除/启用/禁用多个源，URL 重复校验
- **Cron 定时任务**：标准 5 段表达式（分 时 日 月 周），支持 `*` `,` `-` `/`，常用预设一键填充，未来 3 次运行时间预览
- **手动触发**：一键运行/取消，实时日志流（扫描进度、测速结果、Top 排名），防重复运行
- **失败自动重试**：API 源获取/测试失败（退出码非 0 或无有效节点）时自动重试，默认重试 3 次、间隔 5 秒（均可配置）；日志记录每次尝试，详情页展示尝试次数与失败原因
- **历史节点复测**：每轮测速的达标节点自动入池（记录原始来源，归属固化），下轮作为第一个"源"与各 API 源一起复测、合并排名；连续 K 次不达标自动淘汰、超出滚动窗口自动过期、容量上限挤出最慢节点；API 源全部失效时池内节点仍可兜底出结果
- **自动排序提取**：所有源测完后按下载速度降序合并去重，**IPv4 与 IPv6 按地址族分开各自提取**前 N 个（默认 20）节点，互不挤占名额
- **双格式输出（双栈分开）**：
  - `top_nodes.txt` / `top_nodes.yaml` / `all_sorted.txt`：仅 IPv4 节点
  - `top_nodes_v6.txt` / `top_nodes_v6.yaml` / `all_sorted_v6.txt`：仅 IPv6 节点（官方优选等）
  - TXT 格式为 `ip:port#速度-数据中心-位置`（纯数据行，无注释）；YAML 为 Clash trojan 节点（password/sni/ws-opts 可在界面配置）
- **分源优选**：测速完成后每个源单独提取速度最快的前 N 个节点（默认 5，可配置），输出 `top_by_source.txt` 与对应的 Clash YAML `top_by_source.yaml`（节点名带 `[源名]` 前缀），运行详情页同步展示分源排名表格
- **latest 固定目录**：每次任务成功后自动把最新结果同步到 `results/latest/`，路径永不变化，可直接作为订阅链接（v4 / v6 分开订阅）
- **Top 节点质检**：每次对完整任务提取的原始 Top 节点（v4 + v6 双栈）全量复测（固定基准不随剔除缩小，被剔除节点速度恢复自动回归），实测不达标或未出现在结果中的节点分别从对应栈的订阅文件剔除；支持独立 cron 高频保鲜，只重写 latest 两套 Top 文件，不触碰运行历史与历史节点池
- **官方 IPv6 优选（可选）**：开启后每轮任务附加一个「官方IPv6优选」伪源，分两阶段执行——阶段1 用 cfdata 官方模式（`-mode=official -offiptype=6 -offdc=NONE -offspeedlimit=0`）对 Cloudflare 官方 IPv6 地址库（约 633 条）只做**延迟扫描**，导出全部延迟达标节点（覆盖所有机房，不被官方模式"自动选最低延迟机房"收敛掉）；阶段2 把这些节点写入临时文件 `official_v6_input.txt`，用非标模式（`-nsbfile`）**按 IP 逐个测速**，速度达标节点进入 IPv6 侧独立提取输出（`top_nodes_v6.*`）、参与分源 Top 与质检，但**不进历史节点池**；无 API 源时也可单独跑。官方 v6 的延迟阈值（`-offdelay`，阶段2 以 `-nsbdelay` 同值复测）与达标上限（`-nsbspeedlimit`）均可在界面配置
- **历史记录**：每次运行保存各源测试情况、Top 节点表格（v4/v6 分表）、分源最快节点表格、YAML 预览（v4/v6 双份），支持在线下载

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
2. **参数设置**：按需调整测速线程数（`-nsbspeedtest`）、最低速度（`-nsbspeedmin`）、提取节点数、分源提取数量（默认每源最快 5 个）、源失败重试次数（默认 3 次、间隔 5 秒）等
3. **定时任务**：配置 cron 表达式实现全自动定时优选（如 `0 8 * * *` 每天 08:00）
4. **手动触发**：点击「手动运行」立即测试，实时查看日志
5. **运行结果**：下载 `top_nodes.txt` / `top_nodes.yaml` / `top_by_source.txt`，或查看 Top 节点与分源最快节点详情

### latest 固定目录（订阅用法）

每次任务成功后，最新结果会自动同步覆盖到固定目录 `results/latest/`（**IPv4 与 IPv6 分开输出**）：

```
results/latest/
├── top_nodes.txt       # Top 节点 TXT 格式（仅 IPv4）
├── top_nodes.yaml      # Top 节点 Clash YAML 格式（仅 IPv4）
├── all_sorted.txt      # 全量排序结果（仅 IPv4）
├── top_nodes_v6.txt    # Top 节点 TXT 格式（仅 IPv6，官方优选等）
├── top_nodes_v6.yaml   # Top 节点 Clash YAML 格式（仅 IPv6）
├── all_sorted_v6.txt   # 全量排序结果（仅 IPv6）
├── top_by_source.txt   # 分源最快节点（每源前 N 个，按源分段）
├── top_by_source.yaml  # 分源最快节点 Clash YAML
├── qa_input.txt        # 质检基准（v4 + v6 双栈合并的原始 Top 列表）
└── meta.json           # 元信息（来源运行 ID / 完成时间 / v4/v6 节点数）
```

对应固定下载地址（路径不变，内容随每次任务自动更新）：

- `http://服务器IP:8088/api/download/latest/top_nodes.yaml` — IPv4 订阅，可直接填入 Clash 等客户端
- `http://服务器IP:8088/api/download/latest/top_nodes.txt`
- `http://服务器IP:8088/api/download/latest/all_sorted.txt`
- `http://服务器IP:8088/api/download/latest/top_nodes_v6.yaml` — IPv6 订阅（需客户端具备 IPv6 网络）
- `http://服务器IP:8088/api/download/latest/top_nodes_v6.txt`
- `http://服务器IP:8088/api/download/latest/all_sorted_v6.txt`
- `http://服务器IP:8088/api/download/latest/top_by_source.txt`
- `http://服务器IP:8088/api/download/latest/top_by_source.yaml`

「运行结果」页顶部的「最新结果」卡片提供一键下载与复制订阅链接（v4 / v6 分开复制）。

## 历史节点复测

每轮测速结束后，各源达标节点（速度 ≥ 最低速度阈值）自动进入**历史节点池**，持久化于 `data/history_nodes.json`（Docker 挂载 `/app/data` 即跨容器保留）。开启复测后（默认开启），池内节点作为第一个"源"参与下一轮测试，与各 API 源的结果合并排名、参与分源 Top 提取。

节点在首次入池时**固化原始来源归属**（首次发现它的 API 源），复测只刷新速度/命中次数，不改变归属；节点同时在多个源出现时归属列表会累积记录。

淘汰与收敛策略（均可配置）：

| 策略 | 默认值 | 说明 |
| --- | --- | --- |
| 连续失败淘汰 | 3 次 | 复测连续 K 次不达标即移出池（同轮被任一 API 源测达标的节点不计失败） |
| 滚动窗口 | 5 次 | 只保留最近 N 次运行中出现过的节点，超过窗口未出现自动过期 |
| 池容量上限 | 250 个 | 超出后挤出最近速度最慢的节点 |

- 所有 API 源失效（源地址全部不可用）时，历史池非空即可继续出结果，兜底不断更
- 池为空时自动跳过历史源，不影响任务执行；关闭复测且无已启用源时任务拒绝启动
- 界面「参数设置」页提供开关与三项参数配置，「历史节点池」卡片展示池内节点（最近速度/历史最佳/命中次数/失败次数/来源）与来源贡献统计，支持一键清空

## Top 节点质检

完整任务测的是"候选池"，订阅真正用的是 `results/latest/` 的 Top 节点。质检子任务只针对这批节点做高频复测保鲜，不产生新的运行记录：

- **触发方式**：「运行结果」页点「🔍 质检 Top 节点」手动触发，或在「Top 节点质检定时」卡片配置独立 cron（与主任务定时互不影响，如 `*/30 * * * *` 每 30 分钟一次）
- **固定基准**：每次质检都从 `latest/qa_input.txt`（完整任务提取的**原始 Top 节点**，如 20 个）全量复测，不随剔除缩小 —— 上次被剔除的节点速度恢复达标后**自动回归**订阅文件；该基准在每次完整任务成功后随 latest 一起刷新
- **剔除规则**：cfdata 的结果文件只包含**达标节点**（`-nsbspeedmin` 由 CLI 自行过滤），因此——出现在结果中的节点即本次实测达标，保留；复测速度低于「最低速度」（复用参数设置 `speed_min`）或**未出现在结果文件中**的节点（本次测速未达标/失联）一并剔除；被剔除节点仍留在基准中，下次质检达标自动回归
- **双栈剔除**：IPv4 节点从 `top_nodes.txt` / `top_nodes.yaml` 剔除，IPv6 节点从 `top_nodes_v6.txt` / `top_nodes_v6.yaml` 剔除（两套文件都保鲜）
- **速度刷新**：保留节点的注释速度/节点名同步刷新为本次实测值，按速度降序重排
- **影响范围**：只重写 latest 的两套 Top 文件（v4 + v6）并在 `meta.json` 追加 `qa` 信息（含 `kept_v4`/`kept_v6` 双栈计数）；`qa_input.txt` 基准、`all_sorted*.txt`、分源文件、`runs.json` 运行历史、历史节点池全部不动，主任务的合并/排序/提取逻辑不受影响
- **工作目录**：固定 `results/qa/` 只保留最近一次质检的日志/输入导出/实测结果（每次覆盖，旧版本时间戳目录 `qa_*` 首次质检时自动清理）
- **互斥保护**：与主任务共用执行锁，一方运行中另一方触发被拒绝；质检失败或取消时 latest 保持原样
- **记录留痕**：每次质检（成功/失败/取消）记入 `data/qa_runs.json`（保留最近 50 条），界面展示检查/保留/剔除/回归统计与被剔除、回归节点明细
- **全灭提醒**：本次全部未达标时文件清空并记录警告（基准仍在，下次质检仍全量复测、达标即回归），提示尽快触发一次完整任务

等价命令（把原始 Top 节点导出为 `ip:port` 文本后执行）：

```bash
./cfdata-linux-amd64 -cli -mode nsb \
  -nsbfile qa_nodes.txt \
  ... # 其余参数与完整任务相同, speedmin 即剔除阈值
```

## 官方 IPv6 优选（可选）

完整任务默认只测 API 源（及其池化的历史节点）。开启本功能后，每轮任务额外附加一个**「官方IPv6优选」伪源**：用 cfdata 官方模式（`-mode=official -offiptype=6`）直接扫描 CLI 内置的 **Cloudflare 官方 IPv6 地址库**（约 633 条），按延迟阈值筛选后测速，结果**与 IPv4 分开提取与输出**。

- **开关位置**：「参数设置」页「官方 IPv6 优选」卡片，默认关闭；关闭时整体逻辑与原来完全一致
- **参数**：
  - 达标结果上限（`-offspeedlimit`，默认 20）：测速达标达到该数量即停止
  - 延迟阈值 ms（`-offdelay`，默认 500）：延迟超过该值的节点剔除
  - 测速下限复用全局「最低速度」（`-nsbspeedmin` → `-offspeedmin`），线程数复用全局测速线程数
- **双栈分开**：所有测速结果（不区分来源）按地址族拆分——IPv4 各取速度最快的前 N 个写入 `top_nodes.txt` / `top_nodes.yaml` / `all_sorted.txt`，IPv6 各取前 N 个写入 `top_nodes_v6.txt` / `top_nodes_v6.yaml` / `all_sorted_v6.txt`，**互不挤占 Top 名额**，订阅可按栈分开使用；分源 Top（`top_by_source`）仍按源分段（官方 v6 段天然是纯 IPv6）
- **质检保鲜**：v6 节点与 v4 一起进入质检基准（`qa_input.txt`），照常被复测剔除/回归（写入 v6 自己的文件）
- **不进历史池**：官方地址库每轮都从头扫描（地址库本身就是"池"），达标 v6 节点**不写入** `history_nodes.json`，不影响历史池的归属/淘汰/容量统计
- **IPv6 格式约定**：`top_nodes_v6.txt` 与 `qa_input.txt` 中保留方括号 `ipport`（如 `[2606:4700:1111::1]:443`，与 CLI 导出一致，可直接回传 `-nsbfile` 复测）；`top_nodes_v6.yaml` 的 Clash `server` 字段为裸地址（去方括号）
- **无 API 源兜底**：API 源全部删除且历史池为空时，仅开启官方 IPv6 优选也能正常出结果（错误提示同步引导开启）
- **前提**：运行本服务的机器需要具备 IPv6 出口网络，否则官方模式测不出达标节点。**Docker 部署注意**：默认桥接网络没有 IPv6（宿主机有也不行），需为容器启用 IPv6（如 `docker network create --ipv6` 自建网络）或改用 `--network host`；官方源 0 节点时日志会输出该诊断提示
- **全灭保护**：所有源（API 源 + 历史池 + 官方 v6）本轮均无有效节点时，任务直接判失败并提示「latest 订阅文件保持原样未动」——不会用空结果覆盖 `results/latest/` 的两套订阅文件与质检基准

等价命令：

```bash
./cfdata-linux-amd64 -cli -mode=official -offiptype=6 \
  -offout official.csv -offthreads 100 -offport 443 \
  -offdelay 500 -offspeedmin 5 -offspeedlimit 20 \
  -format=csv -fields=ipport,latency,speed,dc,loc,region,city
```

## 目录结构

```
cfdata_web/
├── app.py                 # 主程序（后端 + 静态页面服务）
├── cfdata-linux-amd64     # CFData 二进制（测速引擎）
├── web/
│   └── index.html         # Web 界面（单文件，无构建依赖）
├── data/                  # 数据目录（Docker 环境 = 挂载点 /app/data，需挂载持久化）
│   ├── config.json        # 源/定时/参数/质检定时配置（本地运行时直接生成在应用目录）
│   ├── history_nodes.json # 历史节点池（达标节点 + 来源归属 + 淘汰状态）
│   ├── qa_runs.json       # 质检记录（最近 50 条：检查/保留/剔除统计）
│   ├── cfdata-config.json # cfdata CLI 配置（本地运行时直接生成在应用目录）
│   ├── locations.json     # 数据中心位置缓存（本地运行时直接生成在应用目录）
│   └── GeoLite2-ASN.mmdb  # ASN 数据库共享缓存（首次运行下载一次，之后各任务目录复用不再重复下载）
└── results/               # 运行时自动生成
    ├── runs.json          # 运行记录索引
    ├── latest/            # 最新一次成功任务的固定结果目录（自动覆盖更新）
    │   ├── top_nodes.txt
    │   ├── top_nodes.yaml
    │   ├── all_sorted.txt
    │   ├── top_by_source.txt
    │   ├── top_by_source.yaml
    │   ├── qa_input.txt    # 质检基准（完整任务提取的原始 Top 节点）
    │   └── meta.json      # 元信息（来源运行 ID / 完成时间 / 节点数 / 质检信息 qa）
    ├── 20260901_080000/   # 每次运行一个目录
    │   ├── run.log        # 运行日志
    │   ├── source_01.csv  # 各源原始结果
    │   ├── top_nodes.txt  # Top 节点（TXT 格式）
    │   ├── top_nodes.yaml # Top 节点（Clash YAML 格式）
    │   ├── all_sorted.txt # 全量排序结果
    │   ├── top_by_source.txt # 分源最快节点（每源前 N 个）
    │   └── top_by_source.yaml # 分源最快节点（Clash YAML 格式）
    └── qa/                # 质检固定工作目录（只保留最近一次，不进入运行历史）
        ├── qa.log         # 质检日志（剔除/回归/保留明细）
        ├── qa_nodes.txt   # 质检输入（原始 Top 节点导出）
        └── qa.csv         # 质检实测结果
```

## 输出格式示例

TXT（`top_nodes.txt`）：

```
104.16.132.229:443#42.41MB/s-HKG-HK
91.213.174.82:2053#32.91MB/s-HKG-HK
```

分源最快节点（`top_by_source.txt`，每个源单独按速度降序提取前 N 个，纯数据行无注释）：

```
===== HK 优选 (5) =====
104.16.132.229:443#42.41MB/s-HKG-HK
91.213.174.82:2053#32.91MB/s-HKG-HK
...

===== JP 优选 (5) =====
...
```

分源最快节点 Clash YAML（`top_by_source.yaml`，节点名带 `[源名]` 前缀区分来源）：

```yaml
proxies:
  - name: "[HK 优选] HKG-HK-42.41MB/s"
    server: 104.16.132.229
    port: 443
    type: trojan
    ...
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
| `/app/results` | 测试结果（TXT/YAML/日志/运行记录/latest 固定目录） |
| `/app/data` | **必挂**，持久化 `config.json`（API 源列表、定时任务、参数设置）、`history_nodes.json`（历史节点池）、`cfdata-config.json`、`locations.json` 缓存；不挂载则容器重建后配置与历史池丢失 |

> 重要：所有界面上的配置（添加的 API 源、cron 表达式、参数设置、节点模板）都保存在 `/app/data/config.json`，务必挂载该目录，否则 `docker rm` 后重建容器配置会重置为默认值。旧版本配置存放在容器内 `/app/config.json`，升级镜像后会自动迁移到 `/app/data`。

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

开启历史节点复测时，会在各 API 源之前先对池内节点执行一次（等价于把池导出为 `ip:port` 文本后运行）：

```bash
./cfdata-linux-amd64 -cli -mode nsb \
  -nsbfile history_input.txt \
  ... # 其余参数与上相同
```

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
