# MChat — 面试 Q&A

---

## 项目简介

**MChat** 是一个基于 Netty 的高并发即时通讯 (IM) 平台，目标支持 **10,000+ 并发长连接**。技术栈：Spring Boot 3.2 + Netty 4.1 + Protobuf 3 + Redis。支持 P2P 私聊、群聊、消息 ACK 确认、离线消息、跨服务器路由、心跳检测、性能监控等能力。

**为什么做这个项目**：大部分 Java 项目都是 CRUD + Spring MVC，我想深入理解网络编程——TCP 粘包拆包怎么处理、Netty 的 Reactor 线程模型怎么工作、高并发下消息如何可靠投递、多服务器之间怎么路由消息——这些都是面试和实际工作中会触及的核心能力。

**项目规模**：约 700 行 Java 代码，15 个类，自包含可运行，无需外部数据库。

---

## 面试官可能会问的问题

### Q1：你的自定义协议是怎么设计的？为什么不用 HTTP？

**A**：IM 场景和 HTTP 有本质区别：

| 维度 | HTTP | 自定义二进制协议 |
|---|---|---|
| 连接模型 | 短连接（或仅靠 WebSocket 变长连） | 原生长连接 |
| 头部开销 | 几百字节/请求 | 固定 11 字节 |
| 消息方向 | 客户端主动拉取 | 服务端主动推送 |
| 实时性 | 依赖轮询/WebSocket 升级 | 建连即实时 |

我的协议格式（大端序）：

```
+------------+--------+----------+-------+--------+----------+
| Magic(4B)  | Ver(1B)| Ser(1B)  | Cmd(1B)| Len(4B)| Body     |
+------------+--------+----------+-------+--------+----------+
  0x4D434854    1        1(Proto)    1~6     N字节    Protobuf
```

- **魔数 0x4D434854** (ASCII: MCHT)：防止非法客户端随意发包，首字节校验不通过直接关连接。
- **Length + LengthFieldBasedFrameDecoder**：解决 TCP 粘包/半包——Netty 在攒够完整帧之前不会交给业务层。
- **Protobuf 序列化**：相比 JSON，体积小 30%~50%，解析快 3~10 倍，且强类型避免了字段名拼写错误。
- **固定头 11 字节**：远小于 HTTP 头的数百字节，在 10k 连接下节省显著带宽。

---

### Q2：TCP 粘包/半包是什么？你具体怎么解决的？

**A**：

**粘包**：发送方连续发了两个小包，TCP 把它们合并成一个 TCP 段发出去，接收方一次 read 读到两个包的内容黏在一起。

**半包**：发送方发了一个大包，TCP 分片传输，接收方 read 时只读到包的前半部分。

**我的解决方案**：继承 Netty 的 `LengthFieldBasedFrameDecoder`：

```java
public MessageDecoder() {
    super(1024 * 1024,  // maxFrameLength: 1MB，防止恶意大包撑爆内存
          7,             // lengthFieldOffset: magin(4)+ver(1)+ser(1)+cmd(1)=7
          4);            // lengthFieldLength: len 字段占 4 字节
}
```

工作原理：解码器读到第 7 字节偏移处的 4 字节长度值后，Netty 内部会自动累积后续字节直到凑够 `length` 字节，才将完整帧交给 `decode()` 方法。如果数据不够，返回 `null`，Decoder 自动等待后续数据到达。这就是 **Netty 的 accumulate + frame-based 机制**，完全不用自己处理半包状态机。

---

### Q3：Netty 的线程模型是什么？你做了哪些优化？

**A**：

**Netty 默认 Reactor 主从模型**：

```
BossGroup (1线程)          WorkerGroup (CPU核数线程)
   │                           │
   ├─ accept 新连接 ──────────► ├─ I/O 读写
                                ├─ Pipeline 中执行 Handler
                                └─ ❌ 不能阻塞！（阻塞会拖慢所有该线程上的连接）
```

**我的优化——业务线程池隔离**：

```java
// BusinessThreadPool.java: 核心线程=CPU核数, 最大=核数×2, 有界队列1000
// 原因：消息路由时的 SessionManager 查找、Redis 操作都是同步阻塞的
// 如果放在 EventLoop 线程中执行，会阻塞该 Worker 线程上的所有其他连接
BusinessThreadPool.submit(() -> {
    // 查找目标用户、Redis 存储、离线推送——这些全部异步执行
});
```

关键是：EventLoop 只负责 **I/O 读写的编解码**，解码后的业务逻辑（路由查找、存储、推送）全部提交到业务线程池。这是典型的生产级 Netty 实践。

---

### Q4：心跳机制怎么实现的？为什么不用 TCP 自带的 KeepAlive？

**A**：

TCP 的 `SO_KEEPALIVE` 默认 2 小时才发一次探测包，且只能判断网络连通性，无法感知 **应用层假死**（进程活着但不处理业务）。

我的方案：**Netty IdleStateHandler + 自定义 HeartbeatHandler**。

```java
// NettyServer.java Pipeline:
.addLast(new IdleStateHandler(60, 0, 0, TimeUnit.SECONDS))  // 60s 读空闲触发
.addLast(new HeartbeatHandler())  // 收到空闲事件 → 主动关闭连接
```

工作流程：
1. 客户端每 30s 发一次心跳包（`COMMAND_HEARTBEAT`）
2. `IdleStateHandler` 监测读空闲：收到任何数据都会重置计时器
3. 60s 内无任何数据 → 触发 `READER_IDLE` 事件
4. `HeartbeatHandler.userEventTriggered()` → 关闭连接 → 触发 `channelInactive` → `SessionManager.removeSession()`

**断开检测延迟**：最长 60s（IdleStateHandler 到时间才触发），比 TCP KeepAlive 的 2 小时快得多。

---

### Q5：你的 ACK 确认机制是怎么做的？

**A**：

IM 系统中消息丢失是不可接受的，所以设计了双向 ACK：

```
Client A ──P2P消息(msgId=1001)──► Server ──转发──► Client B
Client A ◄──ACK_OK────────────── Server                │
                                           Client B ──ACK(received)──► Server
```

**服务端→发送方 ACK（可靠性保证）**：
- 消息成功投递到目标 → 返回 `ACK_OK:Delivered`
- 目标离线存 Redis → 返回 `ACK_OK:Stored offline`
- 路由到其他服务器 → 返回 `ACK_OK:Routed to remote`

**客户端→服务端 ACK（去重/重试依据）**：
- 客户端收到消息后发 ACK 给服务端
- 服务端收到客户端 ACK 后可以标记该消息已消费、停止重试

**messageId 设计**：`AtomicLong` 从 `userId * 100000` 起步，保证同一用户的消息 ID 唯一可追踪。生产环境会用雪花算法。

---

### Q6：跨服务器消息路由怎么实现？

**A**：

单机连接数有上限（约 1~2 万/台），水平扩展时需要 **知道目标用户在哪个服务器上**。

**我的方案——Redis 作为路由层**：

```
Server1 (User A 在线)              Redis              Server2 (User B 在线)
     │                                │                       │
     ├─ 发消息给 B                    │                       │
     ├─ GET mchat:online:B ──────────►│                       │
     ├─ 返回 "server-2" ◄─────────────│                       │
     │                                │                       │
     ├─ PUBLISH msg ─────────────────►│                       │
     │                                ├─ SUBSCRIBE ──────────►│
     │                                │   收到消息              │
     │                                │                       ├─ 本地查找 User B
     │                                │                       ├─ writeAndFlush
     │                                │                       └─ 消息投递成功
```

**关键设计**：
1. `mchat:online:{userId}` → 值为服务器 ID，用户登录时 `SET`，断开时 `DEL`
2. `mchat:route:{userId}` → 该服务器订阅的频道，用于接收发给该用户的跨服消息
3. `routeToRemote()` 使用 Redis 底层 `connection.publish()` 发原始字节，不经过序列化器

**回退链**：本地查找 → Redis 路由 → 离线存储，三级保证消息不丢失。

---

### Q7：离线消息怎么存储和投递？

**A**：

**存储**：消息推送时如果目标用户不在本地也不在任何其他服务器，将消息体的 `byte[]` 通过 `RPUSH` 压入 Redis List：`OFFLINE_MSG_{userId}`。

**投递**：用户登录时触发 `deliverOfflineMessages()`：
1. `LRANGE OFFLINE_MSG_{userId} 0 -1` 取出所有消息
2. 逐条通过 `channel.writeAndFlush()` 推送给客户端
3. `DEL OFFLINE_MSG_{userId}` 清空队列

**为什么用 Redis List 而非消息队列**：
- 对单用户的离线消息数量不会特别大，List 足够
- 不需要额外部署 Kafka/RabbitMQ，降低架构复杂度
- 上线时一次性清空，无需维护消费位点

---

### Q8：SessionManager 的 ConcurrentHashMap 为什么能保证线程安全？

**A**：

`ConcurrentHashMap` 在 Java 8+ 使用 **CAS + synchronized 分段锁**：
- 读操作完全无锁
- 写操作只锁对应 bin 的头结点
- 在绝大多数场景下性能接近 HashMap，同时保证 `put/get/remove` 的原子性

这个项目中：
- 多个 Netty Channel 可能同时触发 `channelInactive`（心跳超时关闭）
- 多个用户可能同时登录触发 `addSession`
- 所有操作都并发地操作同一个 `ConcurrentHashMap`

由于 `ConcurrentHashMap` 的每个方法是原子的，不会出现 A 线程 `put`、B 线程 `get` 读到中间状态的问题。

---

### Q9：性能监控具体做了哪些？

**A**：

1. **Netty ByteBuf 泄漏检测**：
```java
ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
```
开发/测试阶段开启 ADVANCED 级别，对 ByteBuf 分配/释放进行采样，防止内存泄漏。

2. **定时指标采集**（`@Scheduled(fixedRate = 30000)`）：
   - 活跃连接数（AtomicLong 增减）
   - 消息吞吐量（IN/OUT rate，通过两次采样差值 ÷ 时间间隔算速率）
   - 堆内存 / 非堆内存 / Netty 直接内存使用量

3. **计数器设计**：全部使用 `AtomicLong`，保证多线程下的计数准确性，避免 `synchronized` 的性能开销。

---

### Q10：优雅关闭怎么做的？

**A**：

两层保证：

1. **JVM Shutdown Hook**：`Runtime.getRuntime().addShutdownHook()` → 关闭 Channel，shutdown EventLoopGroup
2. **Spring @PreDestroy**：Spring 容器关闭时触发

```java
private void shutdown() {
    if (serverChannel != null) { serverChannel.close(); }
    if (bossGroup != null)    { bossGroup.shutdownGracefully(); }
    if (workerGroup != null)  { workerGroup.shutdownGracefully(); }
}
```

`shutdownGracefully()` 会等待正在处理的任务完成再退出，不会丢失正在投递的消息。

---

### Q11：为什么选 Protobuf 而不是 JSON？

**A**：

| 对比 | JSON | Protobuf |
|---|---|---|
| 序列化后体积 | 大（字段名重复传输） | 小（字段名变数字 tag） |
| 解析速度 | 慢（字符串解析） | 快（二进制直接映射） |
| Schema | 无约束，运行时才发现错误 | 编译期 .proto 强校验 |
| 可读性 | 人类可读 | 不可读（需要工具） |
| 适用场景 | Web API | 微服务/IM/游戏 |

IM 场景消息高频、内部通信不需要人类可读——Protobuf 是更优选择。

---

### Q12：如果让你继续完善这个项目，你会做什么？

**A**：

1. **雪花算法 MessageId**：当前 AtomicLong 单机递增，分布式下会冲突
2. **消息持久化**：当前只用 Redis List（内存），可加 RocksDB/MySQL 做持久化存储
3. **消息已读/未读**：在 ACK 基础上扩展已读回执
4. **连接迁移/重连**：客户端断线后自动重连 + Session 恢复
5. **10k 并发压测**：这是 plan.md 里唯一未完成的任务，用 JMeter 或手写 Netty 客户端模拟 10000 连接同时在线发消息
6. **支持 WebSocket 客户端**：让浏览器端也能接入
7. **消息顺序性保证**：单用户单 Channel 天然有序，跨连接场景可加序列号
