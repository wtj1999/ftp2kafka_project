# FTP2Kafka 电池测试数据采集系统

从FTP服务器采集电池测试数据，解析处理后发送到Kafka消息队列的数据采集系统。

## 项目概述

本系统用于自动从FTP服务器获取电池测试设备生成的CSV数据文件，进行解析和格式转换后发送到Kafka消息队列。系统支持锐能和科列两种测试设备的数据格式，并集成CatBoost机器学习模型进行数据预测。

## 功能特性

- **FTP数据采集**：自动轮询FTP服务器，下载当天的测试数据文件
- **文件配对处理**：自动匹配"记录层"和"工步层"成对文件
- **多设备支持**：支持锐能和科列两种测试设备的数据格式
- **数据处理**：将CSV数据转换为JSONL格式，提取电芯电压、温度等关键信息
- **Kafka集成**：将处理后的数据发送到多个Kafka主题
- **数据预测**：使用CatBoost模型进行电池性能预测
- **去重机制**：通过processed.json记录已处理的文件，避免重复处理

## 项目结构

```
ftp2kafka_project/
├── poller.py                    # 主程序入口，轮询控制器
├── requirements.txt             # 依赖包列表
├── docker-compose.yml           # Docker部署配置
├── fetcher/                     # FTP数据采集模块
│   ├── fetcher.py              # FTP采集主逻辑
│   ├── ftp_file.py            # FTP文件列表工具
│   ├── ftp_info.py             # FTP信息获取
│   └── ftp_func.py             # FTP工具函数
├── processor/                   # 数据处理模块
│   ├── parser_to_jsonl.py     # CSV转JSONL主流程
│   ├── jsonl_to_kafka.py      # JSONL发送到Kafka
│   ├── process_csv_parser.py   # 锐能记录层数据解析
│   ├── result_csv_parser.py    # 锐能工步层数据解析
│   ├── process_csv_parser_kelie.py   # 科列记录层数据解析
│   ├── result_csv_parser_kelie.py    # 科列工步层数据解析
│   ├── process_kafka_data.py   # Kafka数据处理和预测
│   ├── pred_data.py            # CatBoost预测模型加载
│   └── parser_csv_name.py      # CSV文件名解析工具
├── catboost_models_packnum_3_cellnum_102/   # 1拖3 102串预测模型
├── catboost_models_packnum_4_cellnum_102/   # 1拖4 102串预测模型
└── data/                        # 数据目录
    └── incoming/
```

## 环境要求

- Python 3.8+
- Docker (可选)
- Kafka消息队列
- FTP服务器

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置说明

在项目根目录创建 `.env` 文件，配置以下环境变量：

```bash
# 日志级别
LOG_LEVEL=INFO

# 轮询配置
POLL_INTERVAL=60              # 轮询间隔（秒）
INITIAL_BACKOFF=5             # 初始退避时间
MAX_BACKOFF=300               # 最大退避时间

# FTP配置
FTP_HOST=your.ftp.server
FTP_PORT=21
FTP_USER=your_username
FTP_PASS=your_password
FTP_ROOT=/                    # FTP根目录
LOCAL_WORKDIR=./tmp_fetch     # 本地临时工作目录
PROCESSED_DB=./processed.json # 已处理文件记录
BRANCH_MAX=14                 # 分支最大深度

# Kafka配置
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
TOPIC_RECORD=topic_record          # 记录层数据主题
TOPIC_STEP=topic_step              # 工步层数据主题
TOPIC_VEHICLE=topic_vehicle         # 车辆码映射主题
TOPIC_PRED=topic_pred               # 预测结果主题
```

## 使用方法

### 本地运行

```bash
python poller.py
```

### Docker部署

```bash
# 构建镜像
docker-compose build

# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f
```

## 工作流程

1. **轮询启动**：程序启动后按配置的间隔轮询FTP服务器
2. **文件发现**：扫描FTP服务器当天的数据目录，查找匹配的文件对
   - 记录层文件：`...@@记录层.csv`
   - 工步层文件：`...@@工步层.csv`
3. **文件下载**：将未处理过的文件对下载到本地工作目录
4. **数据解析**：
   - 解析文件名获取拖数、电芯数等信息
   - 将CSV转换为JSONL格式
   - 提取电芯电压、温度等特征数据
5. **Kafka发送**：
   - 发送记录层数据到 `TOPIC_RECORD`
   - 发送工步层数据到 `TOPIC_STEP`
   - 发送车辆码映射关系到 `TOPIC_VEHICLE`
   - 发送预测结果到 `TOPIC_PRED`
6. **清理**：处理完成后删除本地临时文件

## 数据格式

### 文件命名规则

```
{设备}@{设备ID}@{电池型号}@{测试步骤配置}@{开始时间}@{结束时间}@{通道号}@@{数据类型}.csv
```

示例：
```
锐能@DT24102D-G24-0000024@03HPB0DA0001BWG240000029@330阶梯充一拖四1P102S DCR@20260205212835@20260206031845@通道1@@记录层.csv
```

### Kafka消息示例

记录层消息：
```json
{
  "acquire_time": "2026-02-05 21:28:35.123456",
  "channel_id": "1",
  "step_id": "1",
  "voltage": 350.5,
  "current": 10.2,
  "pack_code": "03HPB0DA0001BWG240000029",
  "vehicle_code": "DT24102D-G24-0000024",
  "BMS_CellVolt1": 3.45,
  "BMS_CellVolt2": 3.46,
  ...
}
```

预测结果消息：
```json
{
  "discharge_energy_ground_truth": 123.45,
  "discharge_energy_pred": 124.12,
  "discharge_capacity_ground_truth": 45.67,
  "discharge_capacity_pred": 46.23,
  "vehicle_dcr_ground_truth": 0.0123,
  "vehicle_dcr_pred": 0.0125,
  "volt_range_14_ground_truth": 0.05,
  "volt_range_14_pred": 0.052,
  "volt_range_15_ground_truth": 0.06,
  "volt_range_15_pred": 0.063,
  "pred_time": "2026-02-06 03:18:45",
  "vehicle_code": "DT24102D-G24-0000024",
  "pack_code": ["pack1", "pack2", "pack3", "pack4"]
}
```

## 预测模型

系统使用CatBoost回归模型预测以下电池性能指标：

- `target_discharge_energy` - 放电能量
- `target_discharge_capacity` - 放电容量
- `target_vehicle_dcr` - 车辆直流电阻
- `target_volt_range_14` - 工步14电压差
- `target_volt_range_15` - 工步15电压差

模型文件存储在 `catboost_models_packnum_X_cellnum_102/` 目录下，根据拖数（1拖3/1拖4）选择对应模型。

### 模型性能评估

基于验证集（样本数：57-58）的模型性能指标如下：

| 预测目标 | 真实值列 | 预测值列 | 样本数 | MAE     | MAPE (%) | R²    |
|---------|----------|----------|-------|---------|----------|-------|
| 放电能量 | discharge_energy_ground_truth | discharge_energy_pred | 57 | 1483.73 | 0.37% | 0.999 |
| 放电容量 | discharge_capacity_ground_truth | discharge_capacity_pred | 57 | 1.50    | 0.45% | 0.985 |
| 车辆DCR | vehicle_dcr_ground_truth | vehicle_dcr_pred | 58 | 0.0027  | 2.83% | 0.923 |
| 电压差14 | volt_range_14_ground_truth | volt_range_14_pred | 57 | 0.0125  | 7.67% | 0.768 |
| 电压差15 | volt_range_15_ground_truth | volt_range_15_pred | 57 | 0.0126  | 13.95% | 0.698 |

**指标说明：**
- **MAE (Mean Absolute Error)**：平均绝对误差，越小越好
- **MAPE (Mean Absolute Percentage Error)**：平均绝对百分比误差，越小越好
- **R² (R-squared)**：决定系数，越接近1表示拟合越好

## 主要依赖

- `confluent-kafka` - Kafka客户端
- `catboost` - CatBoost机器学习模型
- `pandas` - 数据处理
- `numpy` - 数值计算
- `python-dotenv` - 环境变量管理

## 注意事项

1. 确保Kafka服务正常运行且可访问
2. 确保FTP服务器权限配置正确
3. 预测模型文件需要放置在正确位置
4. 本地工作目录需要有足够的磁盘空间
5. 首次运行会创建processed.json用于记录已处理文件

## 故障排查

- **连接FTP失败**：检查FTP_HOST、FTP_PORT、FTP_USER、FTP_PASS配置
- **Kafka发送失败**：检查KAFKA_BOOTSTRAP_SERVERS配置和网络连接
- **预测失败**：检查catboost_models目录下模型文件是否完整
- **文件名解析失败**：确认CSV文件命名符合规范

## 许可证

本项目为内部项目，版权归作者所有。
