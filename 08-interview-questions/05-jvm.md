---
id: "interview-jvm"
title: "JVM 面试六题：从对象存活到容器内存排障"
module: "08-interview-questions"
category: "jvm"
version_target: "HotSpot JDK 17、21，对照 JDK 25"
doc_status: "已完成"
learning_status: "未学习"
importance: "P0"
last_reviewed: "2026-11-24"
tags: ["JVM", "内存", "G1", "生产排障"]
---

# JVM 面试六题：从对象存活到容器内存排障

## 1. 知识概述与业务价值

面试官问“内存满了怎么办”，第一步是确认谁报错。堆分配失败、类元数据增长、容器被终止需要三条不同的证据链。回答要能解释哪条指标会随修复好转。

## 2. 核心概念与关系

| 概念 | 要回答的问题 | 可观察证据 |
| --- | --- | --- |
| Java 堆和 GC 根 | 哪些对象还被谁持有？ | GC 后堆占用、到根的引用路径 |
| 类加载器和元空间 | 哪批类随哪一个加载器存活？ | 已加载/已卸载类数、加载器统计 |
| 本地内存和容器额度 | 进程与容器实际付了多少内存成本？ | RSS、NMT 分类、cgroup 用量与事件 |

## 3. 底层工作机制与设计哲学

GC 从根沿引用图标记可达对象，业务“再也用不到”的对象仍可能被缓存强引用。G1 把堆分成多个 Region，通过并发标记估算老年代各区的回收收益，再在暂停中搬走选定区域的幸存对象。并发标记减少连续停顿，应用仍需留出搬迁空间。[可达性](https://docs.oracle.com/en/java/javase/21/gctuning/other-considerations.html) · [G1 工作周期](https://docs.oracle.com/en/java/javase/21/gctuning/garbage-first-g1-garbage-collector1.html)

```mermaid
flowchart LR
    Roots["GC 根"] --> Worker["线程池工作线程"]
    Worker --> Entry["ThreadLocalMap 中的条目"]
    Entry --> Buffer["报表缓冲区"]
```

沿这条强引用链，工作线程空闲时缓冲区也可能存活。移除请求上下文后，若其余强引用都已释放，缓冲区才有机会被回收。

## 4. 高频复杂业务场景实操与落地

> [!NOTE] 案例性质说明：以下为大促教学推演。

平台每日处理千亿条事件，与每个在线实例的活跃对象量之间还隔着分区、并发和批大小。假设每个报表请求持有 8 MiB 临时缓冲区，100 个同时工作的请求仅这项就占约 800 MiB。先限制并发并把批量导出切成小块，然后用峰值流量回放验证吞吐、分配速率和 P99。

## 5. 生产异常故障深度排查与根因分析

1. 有 Java 异常时保留**完整错误详情**。堆 OOM 看同负载下 GC 后存活集是否持续增长。元空间 OOM 看是否有旧加载器被长期引用。直接缓冲区分配失败检查缓冲池的用量和未归还的缓冲。
2. 只有退出码 137 时先查容器终止原因、节点事件和内存计数器。SIGKILL 也可能来自超时强停。确认 OOMKilled 后比较容器上限、相邻进程或文件缓存的计费、堆和本地内存的趋势。
3. 服务仍在线时优先读取 GC 日志和已有录制，先隔离单实例再决定是否取堆转储。堆转储可能产生停顿和大量磁盘写入。[Oracle 故障诊断](https://docs.oracle.com/en/java/javase/21/troubleshoot/troubleshooting-memory-leaks.html) · [jcmd 命令与影响](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jcmd.html) · [Kubernetes 容器内存限制](https://kubernetes.io/docs/tasks/configure-pod-container/assign-memory-resource/)

## 6. 技术选型对比、架构演进与成本ROI

批处理追求单位 CPU 的完成量，可用 Parallel GC 与 G1 做对照。在线读服务若 GC 暂停已占去大部分延迟预算，再压测 G1 与 ZGC。JDK 21 可选择分代 ZGC，JDK 24 起 ZGC 只保留分代实现。记录实际收集器和版本，一起比较业务 P99、有效吞吐、CPU、内存余量和分配等待。[收集器选型](https://docs.oracle.com/en/java/javase/21/gctuning/available-collectors.html) · [JDK 21 变更](https://docs.oracle.com/en/java/javase/25/migrate/significant-changes-jdk-21.html) · [JDK 24 ZGC](https://docs.oracle.com/en/java/javase/24/gctuning/z-garbage-collector.html)

## 7. 技术经理与架构师实战管控

在线查询与批量回补分别设容器配额。入口再按租户限制并发和缓冲量，避免一个租户的大查询耗尽在线实例。超出预算时先暂停回补、保留核心读流量。参数变更单同时提交同负载的前后数据与回滚条件。

## 8. 互联网大厂高频面试真题库

以下六题按候选人自述转述考点。题后的数据平台情境与管理追问是本库扩展。可由面经确认的是当次考察，样本不足以统计出题频率。

1. **Q1** JVM 有哪些运行时内存区域？元空间和永久代是什么关系？[美团 2025 暑期一面](https://www.nowcoder.com/discuss/741781901794426880) · [阿里钉钉三面](https://www.nowcoder.com/discuss/353157830410706944)
2. **Q2** 一个类从加载到初始化经过什么过程？为什么类加载器通常先委派给父加载器？[美团到家一面](https://www.nowcoder.com/feed/main/detail/0b920842a9dc4a059497bbdc6732a4a5) · [美团成都一面](https://www.nowcoder.com/discuss/353158294153928704)
3. **Q3** GC Roots 是什么？JVM 怎么据此识别可回收的对象？[美团一面，见作者的美团段落](https://www.nowcoder.com/discuss/353157830410706944)
4. **Q4** G1 怎样回收内存？哪些阶段会暂停应用线程？[美团成都一面](https://www.nowcoder.com/discuss/353158294153928704) · [美团到家一面](https://www.nowcoder.com/feed/main/detail/0b920842a9dc4a059497bbdc6732a4a5)
5. **Q5** 线上遇到 OOM，你如何排查？[美团成都一面，Java 基础部分](https://www.nowcoder.com/discuss/353158294153928704)
6. **Q6** 你会怎么做 JVM 垃圾回收调优？[字节一面，见作者的字节段落](https://www.nowcoder.com/discuss/353157830410706944)

## 9. 结构化深度参考答案

### 题目1 运行时数据区与元空间

#### 简洁回答

线程各有程序计数器和 JVM 栈，执行本地方法的实现还可能使用本地方法栈。堆供线程共享，承载对象和数组。规范中的方法区保存类级结构。HotSpot 在 JDK 8 移除永久代，把类元数据迁至本地内存中的元空间。

#### 深入回答

每次调用方法会建立栈帧，容纳局部变量、操作数栈等。一个局部变量可以保存对堆对象的引用。运行时常量池是方法区的一部分，字符串对象本身可在堆上。直接缓冲区的 Java 包装对象在堆上，缓冲内容可占用堆外空间。这与 JMM 定义的线程可见性属于两个不同问题。[JVM 规范 §2.5–2.6](https://docs.oracle.com/javase/specs/jvms/se21/html/jvms-2.html) · [Oracle 元空间诊断](https://docs.oracle.com/en/java/javase/21/troubleshoot/troubleshooting-memory-leaks.html)

#### 架构师/技术管理视角

规则平台频繁热加载类时，把堆、元空间和类加载器数量分开监控。若每次规则更新后类数量只增不减，就关注加载器的引用链。盲目增大堆会挤压容器剩余预算。

### 题目2 类加载与委派

#### 简洁回答

普通类先加载，再链接和初始化。链接包含验证、准备、解析，部分符号解析可延后。默认委派流程先查自己已加载的类，再请父加载器处理，最后才查找本地定义。

#### 深入回答

准备阶段为静态字段分配存储并设置初始默认值。初始化按规范处理常量字段，执行静态初始化逻辑。以普通类为例，运行时类型由二进制名和定义它的类加载器共同确定。两个插件各自定义同名接口，即使字节相同，跨边界强转也可能失败。JDK 17/21 的常用层次为引导、平台和应用类加载器，自定义加载器也可采用不同的委派策略。[JVM 规范第 5 章](https://docs.oracle.com/javase/specs/jvms/se21/html/jvms-5.html) · [默认委派顺序](https://docs.oracle.com/en/java/javase/21/security/java-se-platform-security-architecture.html)

#### 架构师/技术管理视角

连接器允许插件使用自己的依赖版本时，共享 API 由父加载器定义，私有实现留在子加载器。发布验收同时验证两版插件共存、类型转换以及卸载后类加载器能否释放。

### 题目3 可达性与 GC Roots

#### 简洁回答

活跃线程持有的引用、JNI 全局引用等都可成为遍历起点。两个对象即使互相引用，只要外部再无强引用路径到达它们，该循环本身也无法使它们保持强可达。

#### 深入回答

沿着根到对象的强引用逐层跟踪即可定位保留原因。例如工作线程仍活跃，它的 ThreadLocal 值持有一个报表缓冲区，业务处理结束后这条链可能仍存在。弱引用、软引用等另有处理语义，对象失去强可达性也没有约定的回收时刻。[GC 根与可达性](https://docs.oracle.com/en/java/javase/21/gctuning/other-considerations.html) · [JDK 引用语义](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/ref/package-summary.html)

#### 架构师/技术管理视角

故障报告写出具体持有者、生命周期和修复后的趋势。例如请求结束时在 finally 中清理请求上下文，在相同负载下验证 GC 后存活集趋稳。在只有“某类型占用最大”时继续找持有它的根。

### 题目4 G1 的停顿与并发

#### 简洁回答

年轻代疏散（evacuation）要暂停应用并复制存活对象。启动并发标记的初始工作搭在一次年轻代停顿上；主要标记工作与应用并发。Remark 和 Cleanup 有停顿。之后的 Mixed GC 在停顿中同时疏散年轻代和部分老年代 Region。

#### 深入回答

记忆集帮助找到从回收集外指向集内的引用，并发标记估算老区存活量。G1 依据可回收空间和预计耗时挑选老区。目标停顿时间是软目标。存活对象太多、空闲区域不足或巨型对象需要连续 Region 都会使表现恶化，严重时可出现 Full GC。[G1 工作周期与疏散失败](https://docs.oracle.com/en/java/javase/21/gctuning/garbage-first-g1-garbage-collector1.html)

#### 架构师/技术管理视角

把日志按 Young、Mixed、Full 暂停类型分组，再叠加分配速率、疏散耗时及流量峰值。若原因是某租户一次反序列化整份导出，用批大小或流式解析做对照实验通常比反复调目标停顿更直接。

### 题目5 OOM 定位

#### 简洁回答

先拿到出错实例、时间和具体错误详情。堆 OOM、元空间 OOM、直接内存分配失败各有取证方法。看到进程退出时，先核对是否由容器或操作系统终止。

#### 深入回答

堆分支区分一次性大分配、流量导致的真实存活集增长和长期保留，再结合堆样本或转储找持有链。元空间分支比较加载器的数量及其加载的类。本地内存分支对照直接缓冲池与操作系统数据，若事先开启 NMT，可比较 `VM.native_memory baseline` 与 `summary.diff`。NMT 只跟踪 HotSpot 覆盖的分配，第三方本地内存仍要单独检查。[Oracle 内存泄漏诊断](https://docs.oracle.com/en/java/javase/21/troubleshoot/troubleshooting-memory-leaks.html) · [NMT 的覆盖范围](https://docs.oracle.com/en/java/javase/21/vm/native-memory-tracking.html)

#### 架构师/技术管理视角

先把受影响分片的负载降到能留证据的水平，保护同池租户。把转储容量、访问权限、留存期限纳入应急方案。修复验收覆盖原高峰以及高峰后回落阶段，确认内存真的释放。

### 题目6 GC 调优与验收

#### 简洁回答

先问业务哪里慢，收集器是什么，慢请求是否和 GC 同时发生。确认关联后再决定是减少分配、缩短对象存活时间、调整堆余量，还是更换收集器。

#### 深入回答

取相同时间窗的暂停事件、GC CPU、分配速率、回收后堆量和应用请求轨迹。若暂停变短而接口仍慢，继续核对排队或数据库等待。若内存回收跟不上分配，同时 CPU 已耗尽，提高并发 GC 线程数可能进一步挤占业务 CPU；需检查限额和内存余量。用单一变量回放同一批流量并留出预热时间。[Oracle 收集器选型](https://docs.oracle.com/en/java/javase/21/gctuning/available-collectors.html) · [Oracle ZGC 容量考量](https://docs.oracle.com/en/java/javase/21/gctuning/z-garbage-collector2.html)

#### 架构师/技术管理视角

按在线延迟预算和离线批次完成时间分别验收。即使 P99 改善，也计算相同吞吐所需的实例数。让灰度与对照组处理同类型租户，同时记录错误率、CPU 和每千次成功请求成本，再决定是否推广。

## 10. 知识自检与口述推演清单

- [ ] 给出 137 和正常堆曲线，指出还欠缺哪两项证据。
- [ ] 在一张 G1 时间线上标出并发段和至少两处停顿。
- [ ] 用一个缓存或线程池例子画出从 GC 根到业务对象的完整引用链。

## 11. 知识脉络与技术图谱关联

用 JVM 原理页补齐 happens-before、并发屏障和容器容量规划，再用 Spring 题库复习容器如何持有单例对象。[JVM 原理与排障](../03-java-engineering/01-jvm-memory-gc.md) · [Spring 框架六题](./06-spring.md)

## 12. 权威参考与官方进阶资料

语言级内存模型见 [JLS §17.4](https://docs.oracle.com/javase/specs/jls/se21/html/jls-17.html#jls-17.4)。运行时结构与实现细节分别查 [JVM 规范](https://docs.oracle.com/javase/specs/jvms/se21/html/index.html) 和 [HotSpot GC 指南](https://docs.oracle.com/en/java/javase/21/gctuning/)。
