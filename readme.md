# Flink实例 v1.12版本(更新中)
``` 
1.流和批处理
2.Source数据源
3.Sink
4.operator 操作算子
5.侧输出流
6.window
    Keyed Windows
    stream
           .keyBy(...)               <-  keyed versus non-keyed windows
           .window(...)              <-  required: "assigner"
          [.trigger(...)]            <-  optional: "trigger" (else default trigger)
          [.evictor(...)]            <-  optional: "evictor" (else no evictor)
          [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
          [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
           .reduce/aggregate/fold/apply()      <-  required: "function"
          [.getSideOutput(...)]      <-  optional: "output tag"

    Non-Keyed Windows
    stream
           .windowAll(...)           <-  required: "assigner"
          [.trigger(...)]            <-  optional: "trigger" (else default trigger)
          [.evictor(...)]            <-  optional: "evictor" (else no evictor)
          [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
          [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
           .reduce/aggregate/fold/apply()      <-  required: "function"
          [.getSideOutput(...)]      <-  optional: "output tag"
7.event time
    a.watermark
        Watermark = 进入 Flink 的最大的事件时间（mxtEventTime）— 指定的延迟时间（t）
        如果有窗口的停止时间等于或者小于 maxEventTime – t（当时的 warkmark），那么 这个窗口被触发执行。 注意：Watermark 本质可以理解成一个延迟触发机制。
    b.allowedlateness
8.TableAPI  
9.SQL  
10.Complex event processing复杂事件处理CEP  
11.Apache Iceberg
```
