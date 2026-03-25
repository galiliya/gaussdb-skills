# Gauss DB Local Skill

一个可公开分享的 Codex skill，用于通过本地 CLI 查询 Gauss 数据库。

这个公开版做了两件事：

- 不包含任何真实数据库地址、用户名、密码。
- 默认配置只需要填写主机 IP、用户名、密码；端口和数据库名都有默认值。

## 仓库内容

- `SKILL.md`：给 Codex 的 skill 说明。
- `tools/gauss-db`：命令行入口。
- `tools/src/GaussDbCli.java`：JDBC 查询实现。
- `.codex-local/gauss-db.properties.example`：示例配置。

## 快速开始

1. 复制示例配置：

   ```bash
   cp .codex-local/gauss-db.properties.example .codex-local/gauss-db.properties
   ```

2. 只改下面三项：

   ```properties
   db.host=10.0.0.10
   jdbc.username=omm
   jdbc.password=replace_me
   ```

3. 运行命令：

   ```bash
   ./tools/gauss-db tables
   ./tools/gauss-db describe --table schema.table
   ./tools/gauss-db sql --sql "select * from schema.table fetch first 10 rows only"
   ```

## 配置说明

默认情况下你只需要改三项：

- `db.host`
- `jdbc.username`
- `jdbc.password`

可选项：

- `db.port`：默认 `1888`
- `db.database`：默认 `postgres`
- `jdbc.url`：如果你想完全手写 JDBC URL，可以直接填这个，优先级最高
- `jdbc.driver.class`：如果你要强制指定驱动类，可以手工填

## 驱动处理逻辑

脚本按下面顺序寻找驱动 jar：

1. `GAUSS_JDBC_JAR` 环境变量
2. 仓库内 `drivers/` 目录
3. 本机 Maven 缓存
4. 如果前面都没有，再自动下载公开可用的 `opengaussjdbc` 到 `.codex-local/lib/`

说明：

- 你当前本地使用的 `ZenithDriver` 来自内部 Maven 仓库，不适合直接跟公开仓库一起发布。
- 如果你的环境依赖 `jdbc:zenith` 这类内部连接方式，建议把对应 jar 放到 `drivers/` 目录，或者用 `GAUSS_JDBC_JAR` 指向它。
- 如果脚本自动下载的是公开 `opengaussjdbc`，默认会按 OpenGauss 风格拼接 JDBC URL。

## 安全建议

- 不要把真实的 `.codex-local/gauss-db.properties` 提交到 git。
- 不要把私有驱动 jar 直接提交到公开仓库，除非你确认有分发权限。
