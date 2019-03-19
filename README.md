# 自己模仿codis原理做的 influxdb 集群方案，网上都没搜到有类似想法的，但是这个方案可行
I have imitated the influxdb clustering scheme based on the principle of codis. I have not found any similar ideas on the Internet, but this scheme is absolutely feasible and pays tribute to codis.

新特性：
1 同张图上显示 timeshift n hours的数据 和当前数据，可以同张图上 对比历史数据
  对比https://github.com/thedrhax-dockerfiles/influxdb-timeshift-proxy
  原生的influxdb不支持timeshift函数吧,我们这里可以实现
  
2 支持udp server, influxdb原始udp server 是单个goroutine接收数据，改为多goroutine监听同一个端口 增加并发处理程度，udp-server配置来源zk，不需要重启proxy更改配置

New features: 

1 The timeshift n hours data and current data are displayed on the same graph, which can be compared with the historical data on the same graph. https://github.com/thedrhax-dockerfiles/influxdb-timeshift-proxy The native influxdb does not support timeshift. Function, we can achieve here


2 利用codis原理 sharding measurement to 不同的slot, 不同的slot属于不同 influxdb实例，从而达到打散数据的目的，
  支持 influxdb 多个备份同时写（多备份节点）

Using the principle of codis sharding measurement to different slots, different slots belong to different influxdb instances, so as to achieve the purpose of breaking up data, support influxdb multiple backups at the same time write (multiple backup nodes)

3 redirect query by the measurement in the query-sql and merge result together

4 支持用measurement单独sharding和 measurement + 指定tags sharding

support with measurement alone sharding and measurement + specify tags sharding

当前实现的 1 2 3 4 点已经完全满足influxdb 打散数据的作用，而且线上集群跑的很开心，最大的集群最大速率30～40W point／ per sec， 多个proxy实例前边放了http的LB，多个proxy后边都是一样配置的influxdb集群

The current implementation of 1 2 3 4 points has fully satisfied the role of influxdb to break up data, and the online cluster runs very happy, the largest cluster maximum rate of 30 ~ 40W point / per sec, multiple proxy instances before the http LB , multiple proxy are behind the same configuration of the influxdb cluster

查询是拿 query中的 measurement来 获取slot从而 redirect 请求的, 查询语法和 写入数据语法 和 原生的保持一致，所以已让influxdb+grafana 用户 平滑迁移到 集群方案来了

Take the measurement in the query to get the slot and redirect request. The query syntax and the write data syntax are the same as the influxdb, so the influxdb+grafana user has been smoothly migrated to the cluster solution.

看到的喜欢的 有 兴趣 一起开发哇，还有其他一些特性在增加中

