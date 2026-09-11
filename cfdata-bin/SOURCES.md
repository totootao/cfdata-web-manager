# 内嵌的 cfdata 二进制来源说明

本目录预置了 [PoemMisty/CFData-WEB](https://github.com/PoemMisty/CFData-WEB) 的官方多平台二进制，
在 CI 构建时被直接复制并 `--add-binary` 内嵌进各平台的可执行文件，**构建过程不再需要联网下载 cfdata**。

## 版本与许可

- 版本：**v1.7.11**（与 `build-binaries.yml` 中的 `CFDATA_REF` 保持一致）
- 许可：**GPL-3.0**（上游仓库 LICENSE）
- 对应源代码：<https://github.com/PoemMisty/CFData-WEB/tree/v1.7.11>

> 本程序以**独立子进程**方式调用 cfdata（通过 `subprocess` 执行其 CLI），
> 不构成对 cfdata 源代码的派生（derivative work）或链接（linking）。

## 文件清单

| 文件 | 平台 | 来源 |
| --- | --- | --- |
| `cfdata-linux-amd64` | Linux x86_64 | 上游官方 release 二进制 |
| `cfdata-linux-arm64` | Linux ARM64 | 上游官方 release 二进制 |
| `cfdata-darwin-amd64` | macOS Intel | 上游官方 release 二进制 |
| `cfdata-darwin-arm64` | macOS Apple Silicon | 上游官方 release 二进制 |
| `cfdata-windows-amd64.exe` | Windows x64 | 上游官方 release 二进制 |
| `cfdata-windows-arm64.exe` | Windows ARM64 | 上游官方 release 二进制 |

## 上游未发布的平台（由 CI 从源码交叉编译，不在此目录）

以下平台上游未提供官方二进制，CI 中通过 `CGO_ENABLED=0` 从 GPL-3.0 源码交叉编译后内嵌：

- `windows-x86`：`GOOS=windows GOARCH=386`
- `linux-armv7`：`GOOS=linux GOARCH=arm GOARM=7`
- `freebsd-amd64`：`GOOS=freebsd GOARCH=amd64`

如需自行更新这些二进制，可直接替换本目录中对应文件（保持文件名不变），重新触发构建即可。
