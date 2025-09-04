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
    - [x] 实现自动发送功能。
    - [x] 为每个 Topic 实现独立的自动发送控制。
    - [x] 新增：实现多 Tab 管理 Topic Producer 功能。
- **消费者 (Consumer) 模块：**
    - [x] 创建多个 Consumer 实例，可属于不同的消费者组。
    - [x] 可配置 `group.id`, `auto.offset.reset`。
    - [x] 订阅指定 Topic。
    - [x] 实时显示接收到的消息。
- **高级监控功能：**
    - [x] 吞吐量监控（系统总体、Topic级别、生产者/消费者级别）
    - [x] 延迟监控（P50、P95、P99延迟）
    - [x] Broker指标监控（CPU、内存、磁盘、网络流量）
    - [x] 监控数据持久化存储
    - [x] 可视化监控仪表板

### 4. 进阶功能 (待定)

- [x] 分区可视化展示。
- [x] 实时吞吐量和延迟监控。
- [x] 消费者组负载均衡可视化。
- [x] 消息浏览功能。

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

- [x] UI 布局更新：在生产者面板中添加"自动发送设置"区域，包括以下控件：
  - [x] 一个用于输入每秒消息数的文本框。
  - [x] 一个用于选择数据类型的下拉框（字符串或 JSON）。
  - [x] 一个用于输入随机 key 长度的文本框（当选择随机 key 时启用）。
  - [x] 一个用于输入随机 JSON 字段数量的文本框（当选择 JSON 类型时启用）。
  - [x] 一个用于显示已发送消息总数的标签。
  - [x] "开始自动发送"和"停止自动发送"按钮。
- [x] 后端逻辑实现：
  - [x] 在 MainController.java 中添加 Map<String, ScheduledExecutorService> 来管理每个 Topic 的独立自动发送任务。
  - [x] 实现 onStartAutoSendButtonClick() 方法，该方法根据 UI 配置启动定时任务。
  - [x] 实现 onStopAutoSendButtonClick() 方法，该方法安全地关闭对应 Topic 的定时任务。
  - [x] 实现辅助方法以生成随机字符串和 JSON 数据。
  - [x] 确保在 UI 线程上更新消息计数器和日志。

### 第六阶段：消费者组模块重构

- [x] 创建独立的消费者组管理FXML文件（consumer-group-management.fxml）。
- [x] 实现ConsumerController类，将消费者组管理逻辑从MainController中分离出来。
- [x] 更新ControllerRegistry，添加对ConsumerController的支持。
- [x] 修改MainController，移除ConsumerGroupUIManager相关代码，添加对ConsumerController的支持。
- [x] 修复main-view.fxml中的fx:id命名冲突问题。

### 第七阶段：多 Tab 管理 Topic Producer 功能

#### 7.1 设计与规划
- [x] 确定使用 Tab 管理不同 Topic 的 Producer 的方案
- [x] 设计 TopicProducer 和 TopicProducerConfig 数据结构
- [x] 设计 MultiTopicProducerController 和 TopicProducerController 控制器类

#### 7.2 创建数据模型
- [x] 创建 TopicProducerConfig 类，封装生产者配置
- [x] 创建 TopicProducer 类，封装每个 Topic 的生产者和相关状态
- [x] 实现生产者的初始化、配置更新和关闭方法

#### 7.3 创建 FXML 文件
- [x] 创建 multi-topic-producer.fxml，包含 TabPane 和添加/删除 Topic 的按钮
- [x] 创建 topic-producer.fxml，作为单个 Topic 的生产者配置界面（基于现有的 producer-management.fxml 修改）

#### 7.4 实现控制器
- [x] 实现 MultiTopicProducerController 类，管理多个 Topic 的 Tab
  - [x] 实现 addTopicTab 方法，添加新的 Topic Tab
  - [x] 实现 removeTopicTab 方法，移除 Topic Tab
  - [x] 实现 getTopicProducer 方法，获取指定 Topic 的生产者
- [x] 实现 TopicProducerController 类，控制单个 Topic 的生产者
  - [x] 实现消息发送方法
  - [x] 实现生产者配置更新方法
  - [x] 实现自动发送开始和停止方法

#### 7.5 集成到主应用
- [x] 修改 MainController，集成 MultiTopicProducerController
- [x] 更新 main-view.fxml，替换现有的生产者面板为多 Tab 生产者面板
- [x] 更新 ControllerRegistry，添加对 MultiTopicProducerController 的支持

#### 7.6 测试与优化
- [x] 测试多 Topic 生产者的基本功能
- [x] 测试不同 Topic 之间的配置隔离
- [x] 测试同时向多个 Topic 发送消息的功能
- [x] 优化 UI 交互和用户体验
- [x] 添加错误处理和日志记录

### 第八阶段：消费者组手动提交偏移量功能

#### 8.1 设计与规划
- [x] 确定手动提交偏移量的实现方案
- [x] 设计消费者组面板中的手动提交按钮
- [x] 设计线程安全的手动提交机制

#### 8.2 UI 实现
- [x] 在消费者组面板中添加手动提交按钮
- [x] 根据autoCommit参数控制按钮的可见性
- [x] 添加按钮事件处理

#### 8.3 后端实现
- [x] 在ConsumerInstance中添加手动提交方法
- [x] 实现线程安全的手动提交机制
- [x] 在ConsumerGroupManager中添加批量提交方法
- [x] 添加日志记录和错误处理

#### 8.4 测试与优化
- [x] 测试手动提交功能的基本操作
- [x] 测试线程安全性
- [x] 测试与自动提交的兼容性
- [x] 优化用户体验和错误提示

### 第九阶段：支持 Avro 消息类型

#### 9.1 设计与规划
- [x] 确定Avro支持的实现方案
- [x] 设计UI界面和用户交互流程
- [x] 设计Schema管理机制

#### 9.2 UI 实现
- [x] 在自动发送设置中添加Avro类型选项
- [x] 实现Schema输入和编辑界面
- [x] 实现Schema Registry连接配置界面

#### 9.3 后端实现
- [x] 实现Avro序列化器
- [x] 实现Schema管理功能
- [x] 实现Schema验证功能
- [x] 集成Schema Registry（可选）

#### 9.4 测试与优化
- [x] 测试Avro消息的发送和接收
- [x] 测试Schema验证功能
- [x] 测试与Schema Registry的集成
- [x] 优化性能和用户体验

### 第十阶段：国际化支持

#### 10.1 设计与规划
- [x] 确定国际化支持的实现方案
- [x] 识别需要翻译的UI界面和日志内容

#### 10.2 实现国际化
- [x] 将所有中文UI界面和日志内容翻译成英文
- [x] 保持代码注释的中文内容不变

#### 10.3 测试与优化
- [x] 验证翻译后的内容是否正确显示
- [x] 确保翻译后的内容符合国际化标准

### 第十一阶段：高级监控功能 - 基础架构搭建

#### 11.1 设计与规划
- [x] 确定高级监控功能的实现方案
- [x] 设计监控模块的目录结构
- [x] 设计监控数据模型

#### 11.2 创建监控数据模型
- [x] 创建MonitoringData类，封装所有监控数据
- [x] 创建ThroughputData类，表示吞吐量数据
- [x] 创建LatencyData类，表示延迟数据
- [x] 创建TopicThroughputData类，表示Topic吞吐量数据
- [x] 创建BrokerMetricsData类，表示Broker指标数据

#### 11.3 实现数据库操作
- [x] 创建数据库模型类（ThroughputMetric、LatencyMetric、BrokerMetric）
- [x] 实现MetricsDatabase类，提供数据库操作方法
- [x] 创建必要的表和索引

#### 11.4 实现监控服务
- [x] 创建MonitoringService类，定期收集监控数据
- [x] 创建MetricsCollector类，收集和存储监控指标

#### 11.5 创建监控UI
- [x] 创建MonitoringDashboardController类，控制监控仪表板
- [x] 创建monitoring-dashboard.fxml，设计监控仪表板界面

### 第十二阶段：吞吐量监控实现

#### 12.1 设计与规划
- [x] 确定吞吐量监控的实现方案
- [x] 设计吞吐量数据收集机制
- [x] 设计吞吐量数据可视化方案

#### 12.2 修改生产者以收集吞吐量数据
- [x] 在ProducerTabController中添加消息计数器
- [x] 实现吞吐量计算逻辑
- [x] 将吞吐量数据发送到MetricsCollector
- [x] 添加吞吐量UI显示

#### 12.3 修改消费者以收集吞吐量数据
- [x] 在ConsumerInstance中添加消息计数器
- [x] 实现吞吐量计算逻辑
- [x] 将吞吐量数据发送到MetricsCollector
- [x] 添加吞吐量UI显示

#### 12.4 实现吞吐量图表
- [x] 实现系统总体吞吐量图表
- [x] 实现Topic级别吞吐量图表
- [x] 实现生产者/消费者级别吞吐量图表
- [x] 实现图表数据自动更新

#### 12.5 测试与优化
- [x] 测试吞吐量数据收集准确性
- [x] 测试图表显示效果
- [x] 优化数据收集和显示性能

### 第十三阶段：延迟监控实现

#### 13.1 设计与规划
- [x] 确定延迟监控的实现方案
- [x] 设计延迟数据收集机制
- [x] 设计延迟数据可视化方案

#### 13.2 实现消息时间戳机制
- [x] 修改生产者，在消息中添加发送时间戳
- [x] 修改消费者，计算消息延迟
- [x] 实现延迟统计（P50、P95、P99）
- [x] 将延迟数据发送到MetricsCollector

#### 13.3 实现延迟直方图
- [x] 创建延迟直方图数据结构
- [x] 实现延迟数据分布统计
- [x] 将延迟分布数据发送到MetricsCollector

#### 13.4 实现延迟图表
- [x] 实现端到端延迟图表
- [x] 实现延迟分布直方图
- [x] 实现图表数据自动更新

#### 13.5 测试与优化
- [x] 测试延迟数据收集准确性
- [x] 测试图表显示效果
- [x] 优化数据收集和显示性能

### 第十四阶段：Broker指标监控实现

#### 14.1 设计与规划
- [x] 确定Broker指标监控的实现方案
- [x] 设计Broker指标数据收集机制
- [x] 设计Broker指标数据可视化方案

#### 14.2 实现Broker指标收集
- [x] 创建KafkaMetricsCollector类
- [x] 实现Broker连接和指标获取
- [x] 实现CPU、内存、磁盘使用率监控
- [x] 实现网络流量监控
- [x] 将Broker指标数据发送到MetricsCollector

#### 14.3 实现Broker指标表格
- [x] 实现Broker指标数据表格显示
- [x] 实现表格数据自动更新
- [x] 添加指标阈值告警功能

#### 14.4 实现Broker指标图表
- [x] 实现Broker资源使用率图表
- [x] 实现Broker网络流量图表
- [x] 实现图表数据自动更新

#### 14.5 测试与优化
- [x] 测试Broker指标数据收集准确性
- [x] 测试表格和图表显示效果
- [x] 优化数据收集和显示性能

### 第十五阶段：监控仪表板集成与优化

#### 15.1 设计与规划
- [x] 确定监控仪表板的整体布局
- [x] 设计数据刷新机制
- [x] 设计用户交互功能

#### 15.2 集成监控仪表板到主应用
- [x] 修改MainController，添加监控仪表板入口
- [x] 更新main-view.fxml，添加监控按钮
- [x] 实现监控仪表板打开和关闭功能

#### 15.3 实现数据查询功能
- [x] 实现历史数据查询UI
- [x] 实现历史数据查询逻辑
- [x] 实现查询结果可视化

#### 15.4 实现监控配置功能
- [x] 实现监控配置UI
- [x] 实现监控配置保存和加载
- [x] 实现监控参数动态调整

#### 15.5 测试与优化
- [x] 测试监控仪表板整体功能
- [x] 测试数据查询功能
- [x] 测试监控配置功能
- [x] 优化用户体验和性能

### 第十六阶段：告警系统实现

#### 16.1 设计与规划
- [ ] 确定告警系统的实现方案
- [ ] 设计告警规则配置机制
- [ ] 设计告警通知机制

#### 16.2 实现告警规则配置
- [ ] 创建告警规则配置UI
- [ ] 实现告警规则保存和加载
- [ ] 实现告警规则验证

#### 16.3 实现告警检测引擎
- [ ] 创建告警检测引擎类
- [ ] 实现告警规则匹配逻辑
- [ ] 实现告警状态管理

#### 16.4 实现告警通知
- [ ] 实现UI告警通知
- [ ] 实现日志告警记录
- [ ] 实现邮件告警通知（可选）

#### 16.5 测试与优化
- [ ] 测试告警规则配置
- [ ] 测试告警检测功能
- [ ] 测试告警通知功能
- [ ] 优化告警系统性能

---

## 备注

- 如果遇到任何问题或有新的想法，请随时更新此文档。
- 第七阶段是实现多 Tab 管理 Topic Producer 功能的详细计划，包括数据模型设计、FXML 文件创建、控制器实现、集成到主应用以及测试与优化。
- 项目目前已完成第十五阶段的大部分功能，包括高级监控功能的实现与集成，主要剩余第十六阶段的告警系统实现。
