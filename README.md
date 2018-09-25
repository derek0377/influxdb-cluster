# 自己模仿codis原理做的 influxdb 集群方案，网上都没搜到有类似想法的，但是这个方案绝对可行，向codis致敬


新特性：
1 同张图上显示 timeshift n hours的数据 和当前数据，可以同张图上 对比历史数据
  对比https://github.com/thedrhax-dockerfiles/influxdb-timeshift-proxy
  原生的influxdb不支持timeshift函数吧,我们这里可以实现

2 利用codis原理 sharding measurement to 不同的slot, 不同的slot属于不同 influxdb实例，从而达到打散数据的目的，
  支持 influxdb 多个备份同时写（多备份节点）

3 redirect query by the measurement in the query-sql and merge result together

4 支持用measurement单独sharding和 measurement + 指定tags sharding


当前实现的 1 2 3 4 点已经完全满足influxdb 打散数据的作用，而且线上集群跑的很开心，最大的集群最大速率30～40W point／ per sec， 多个proxy实例前边放了http的LB，多个proxy后边都是一样配置的influxdb集群

查询是拿 query中的 measurement来 获取slot从而 redirect 请求的, 查询语法和 写入数据语法 和 原生的保持一致，所以已让influxdb+grafana 用户 平滑迁移到 集群方案来了

看到的喜欢的 有 兴趣 一起开发哇，还有其他一些特性在增加中
