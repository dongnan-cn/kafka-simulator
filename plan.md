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
    - [ ] 新增：实现多 Tab 管理 Topic Producer 功能。
- **消费者 (Consumer) 模块：**
    - [x] 创建多个 Consumer 实例，可属于不同的消费者组。
    - [x] 可配置 `group.id`, `auto.offset.reset`。
    - [x] 订阅指定 Topic。
    - [x] 实时显示接收到的消息。

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

### 第九阶段：支持 Avro 消息类型（规划中）

#### 9.1 设计与规划
- [ ] 确定Avro支持的实现方案
- [ ] 设计UI界面和用户交互流程
- [ ] 设计Schema管理机制

#### 9.2 UI 实现
- [ ] 在自动发送设置中添加Avro类型选项
- [ ] 实现Schema输入和编辑界面
- [ ] 实现Schema Registry连接配置界面

#### 9.3 后端实现
- [ ] 实现Avro序列化器
- [ ] 实现Schema管理功能
- [ ] 实现Schema验证功能
- [ ] 集成Schema Registry（可选）

#### 9.4 测试与优化
- [ ] 测试Avro消息的发送和接收
- [ ] 测试Schema验证功能
- [ ] 测试与Schema Registry的集成
- [ ] 优化性能和用户体验

---

## 备注

- 如果遇到任何问题或有新的想法，请随时更新此文档。
- 第七阶段是实现多 Tab 管理 Topic Producer 功能的详细计划，包括数据模型设计、FXML 文件创建、控制器实现、集成到主应用以及测试与优化。
