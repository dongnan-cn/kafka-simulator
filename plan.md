# JavaFx/Swing Kafka 模拟项目计划书

## 项目目标

使用 JavaFX 或 Swing 构建一个桌面应用程序，模拟并可视化 Kafka 的核心功能，包括生产者、消费者和 Topic 管理。

## 拟定架构

### 1. 技术栈

- **UI 框架：** JavaFX (优先)
- **构建工具：** Maven
- **Kafka 客户端：** `org.apache.kafka:kafka-clients`

### 2. 层次划分

- **用户界面层 (UI Layer)：** 负责用户交互和数据展示。
- **业务逻辑层 (Business Logic Layer)：** 封装 Kafka 客户端逻辑，处理所有业务操作。
- **数据/模型层 (Data/Model Layer)：** 定义数据结构，用于各层之间的数据传输。

### 3. 主要功能模块

- **集群连接配置：**
    - [x] 连接 Kafka 集群并显示 Broker 信息。
- **Topic 管理：**
    - [x] 展示所有 Topic。
    - [x] 创建新 Topic (可配置分区数和副本因子)。
    - [x] 删除 Topic。
- **生产者 (Producer) 模块：**
    - [x] 创建多个 Producer 实例。
    - [x] 可配置 `acks`, `batch.size`, `linger.ms`。
    - [x] 发送消息到指定 Topic。
    - [x] 显示消息发送状态。
    - [ ] 新增：实现自动发送功能。
    - [ ] 新增：为每个 Topic 实现独立的自动发送控制。
- **消费者 (Consumer) 模块：**
    - [x] 创建多个 Consumer 实例，可属于不同的消费者组。
    - [x] 可配置 `group.id`, `auto.offset.reset`。
    - [x] 订阅指定 Topic。
    - [x] 实时显示接收到的消息。

### 4. 进阶功能 (待定)

- [ ] 分区可视化展示。
- [ ] 实时吞吐量和延迟监控。
- [ ] 消费者组负载均衡可视化。
- [ ] 消息浏览功能。

---

## 进度追踪

### 第一阶段：项目基础搭建

- [x] 创建 Maven 项目，并配置依赖。
- [x] 完成基础的 JavaFX 界面框架。
- [x] 实现 Kafka 集群连接和断开的逻辑。
- [x] 编写 `plan.md`。

### 第二阶段：集群元数据可视化

- [x] 实现 Broker 和 Topic 元数据展示。
- [x] 可视化分区（Partition）信息。

### 第三阶段：Topic 管理功能

- [x] 在UI上添加Topic创建和删除的界面。
- [x] 实现创建新Topic的逻辑（可配置分区数和副本因子）。
- [x] 实现删除指定Topic的逻辑。
- [x] 实现刷新Topic列表的功能。

### 第四阶段：生产者与消费者功能

- [x] 在UI上添加生产者（Producer）和消费者（Consumer）面板。
- [x] 实现创建多个Producer实例的逻辑。
- [x] 实现消息发送功能（可配置消息 key, value, acks等）。
- [x] 实现创建多个Consumer实例的逻辑。
- [x] 实现消费者订阅Topic和接收消息的功能。
- [x] 实时展示消息流。

### 第五阶段：高级生产者功能

- [ ] UI 布局更新：在生产者面板中添加"自动发送设置"区域，包括以下控件：
  - [ ] 一个用于输入每秒消息数的文本框。
  - [ ] 一个用于选择数据类型的下拉框（字符串或 JSON）。
  - [ ] 一个用于输入随机 key 长度的文本框（当选择随机 key 时启用）。
  - [ ] 一个用于输入随机 JSON 字段数量的文本框（当选择 JSON 类型时启用）。
  - [ ] 一个用于显示已发送消息总数的标签。
  - [ ] "开始自动发送"和"停止自动发送"按钮。
- [ ] 后端逻辑实现：
  - [ ] 在 MainController.java 中添加 Map<String, ScheduledExecutorService> 来管理每个 Topic 的独立自动发送任务。
  - [ ] 实现 onStartAutoSendButtonClick() 方法，该方法根据 UI 配置启动定时任务。
  - [ ] 实现 onStopAutoSendButtonClick() 方法，该方法安全地关闭对应 Topic 的定时任务。
  - [ ] 实现辅助方法以生成随机字符串和 JSON 数据。
  - [ ] 确保在 UI 线程上更新消息计数器和日志。

---

## 备注

- 如果遇到任何问题或有新的想法，请随时更新此文档。