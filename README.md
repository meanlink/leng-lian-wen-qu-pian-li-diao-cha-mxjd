# 冷链温区偏离调查

分布比较与偏离定位的真实业务分析小型项目。完整模拟客户、痛点、上下游、算法公式与一天工作剧本见 [BUSINESS.md](BUSINESS.md)。

## 模块与业务流程
数据接入（JSON/CSV、字段验证、批次指纹）→ 参数化分析（异步 Python worker）→ 结果复核（指标、图形、明细、解释、批准）→ 定时分发（本地收件箱、CSV/JSON、SHA256）；另有业务总览、审计轨迹和整库备份恢复。
三个贯穿全栈的实体：Dataset 数据批次、Job 分析任务、Report 分发计划；辅助 User、Audit、Delivery、Backup、Restore。

## 整库备份与恢复
负责人（admin）在「备份与恢复」页创建备份、核对备份、选择恢复点并查看进度。
- 备份包包含数据批次、温区调查（分析任务）、分发计划、归档记录、审计与用户，以及对应的归档 CSV/JSON 文件；清单与每个文件均有 SHA256。
- 备份在单个数据库事务中取快照，只捕获快照中已提交分发引用的归档，因此与运行中的计算、分发并发时不会串版本。备份进行中可以继续计算与分发，但不能开始恢复。
- 恢复会先完整核对结构、外键关联与文件指纹；坏包在创建恢复任务前即被拒绝，原库保持不变。
- 恢复期间停止业务写入（503）与分发；切换数据库时提升数据代际，worker 在认领和写回时各核对一次代际，晚到的旧计算会被丢弃，不会污染恢复结果。
- 恢复完成后：备份之后新增的数据不再可见，分发计划默认停用，排队中的调查保留为排队并在恢复后继续执行，恢复前自动生成回退快照。
- 恢复按检查点执行（核对 → 回退快照 → 暂存 → 切换 → 替换归档 → 收尾），中断后可从检查点继续；重试是整表替换，不会重复写入。切换后若失败可回退到恢复前快照。

## 快速启动
```bash
cp .env.example .env
docker compose up -d --build --wait
docker compose exec -T backend python -m scripts.seed_demo
```
访问 http://localhost:18201；后端文档 http://localhost:19201/docs；数据库宿主端口 17701。
`SEED_DEMO=true` 自动幂等 seed；不覆盖用户修改。手动 seed 命令可重复执行。

## 演示账号与权限
|账号|密码|真实权限|
|---|---|---|
|admin@demo.local|Demo123!|接入、执行、批准、分发、暂停、读取与下载|
|analyst@demo.local|Demo123!|接入、执行、读取与下载；不能批准或安排分发|
|viewer@demo.local|Demo123!|只读与下载|

## 技术与本地开发
Python 3.12 / FastAPI / SQLAlchemy / NumPy / SciPy / scikit-learn；MySQL 8.0；Redis 7；Nginx；原生 HTML CSS JavaScript ES modules（用户指定）。后端所有校验、持久化和算法在服务端。无前端打包步骤。
后端：安装 backend/requirements.txt，设置 DATABASE_URL、REDIS_URL、JWT_SECRET、ARTIFACT_DIR、BACKUP_DIR 后在 backend/ 执行 `python -m migrations.initial`、`python -m scripts.seed_demo`、`uvicorn src.main:app`，另开 `python -m src.worker`。备份/恢复测试：在 backend/ 执行 `python -m pytest tests`（使用临时数据库与目录，不影响本地数据）。前端使用带 /api 反向代理的 Nginx，直接打开 index.html 不支持 API。

## 目录与工程约定
backend/src/{models,schemas,services,routes,controllers,middlewares,constants,config,utils}；backend/scripts/seed_demo.py；backend/migrations/initial.py。
frontend/src/{api,stores,types,components/common,hooks,pages,router,utils,constants}。
Role 枚举：admin/analyst/viewer；JobStatus：queued/running/completed/failed。备份/恢复仅 admin 可写，analyst 与 viewer 可读状态。后端 backend/src/constants/enums.py，前端 frontend/src/constants/enums.js；路由权限在 middlewares/auth.py；页面按相同角色控制操作入口。

## 环境变量与 Docker
COMPOSE_PROJECT_NAME=cold-excursion；FRONTEND_PORT=18201；BACKEND_PORT=19201；DB_PORT=17701；DB_PASSWORD、JWT_SECRET 见 .env.example；SEED_DEMO 默认 true。共享命名产物卷供后端下载与 worker 写入，`analysis-backups` 卷保存备份包（`BACKUP_DIR`，默认 /backups），数据库/Redis 各持久卷。
恰好五服务均有健康检查。Redis 队列加 SQL 排队状态兜底；worker 按 next_run 生成本地收件箱文件。周期 60 秒至 30 天。分发结果为批准分析的固定快照；若需新数据重新计算并建立新计划。当前不连接邮件或企业聊天。
错误响应、请求编号、Redis 每 IP 每分钟 240 次限流、JWT 和数据库操作审计。演示密钥与密码仅供本机模拟；生产部署前配置独立密钥与账号。

## 数据与验收
所有数据为固定随机种子生成的模拟业务数据，不能作为实际生产决策证据。项目独立算法、输入边界及业务局限详见 BUSINESS.md。测试与截图统一存放 ../../test-artifacts/gb-1002/，源码不含日志/截图/测试报告。
清理本项目演示数据：`docker compose down -v --remove-orphans`（会删除本项目命名卷）。

## License
MIT，见 LICENSE。
