# delayTask-etcd
Go语言基于ETCD的Watch机制实现的延时任务库，延时任务通过闭包异步调用，可选支持ETCD心跳检测重连。

## 安装

### 下载依赖

```
go get github.com/GalaxyFall/delayTask-etcd@v1.0.1
```

### 导入包

```
import (
	delay_task "github.com/GalaxyFall/delayTask-etcd"
）
```


## 功能列表  
  
- `NewEtcdDelayTask`: 初始化延时任务对象，以前缀key开始监听。  
- `SetDelayTask`: 开始一个延时任务 延时时间到达异步触发设置的回调函数。  
- `CancelDelayTask`: 取消延时任务。 
- `Close`: 取消监听退出。 

## 简单使用
   SetDelayTask ：
   - @param1 任务key
   - @param2 延时任务回调函数
   - @param3 延时时间（秒计算）

```
    cli, _ := etcdInit()

    //获取对象
	task, _ := delay_task.NewEtcdDelayTask(cli, "delay_task")
	//当不需要使用时应取消监听
	defer task.Close()

	fn := func() {
		fmt.Println("delay 5 second say hello")
	}
	
	//5秒后将会看到打印
	_ = task.SetDelayTask("say_hello", fn, 5)

```


## etcd心跳检测断线重连
   开启心跳检测选项，监听协程会每秒检查etcd连接状态，如果连接无效触发重连机制。

```
 	cli, _ := etcdInit()

	//WithEtcdHeartbeat开启心跳检测 传入一个获取etcd新连接的闭包
	task, _ := delay_task.NewEtcdDelayTask(
		cli,
		"delay_task",
		delay_task.WithEtcdHeartbeat(etcdInit),
		delay_task.WithLogger()) //标准输出日志

	fn := func() {
		fmt.Println("delay 10 second say hello")
	}
	_ = task.SetDelayTask("say_hello", fn, 10)

	//操作etcd重连

```



## 注意事项
 - 1.延时时间不宜过小，建议不应在3s内误差可能在大于1s。
 - 2.一个初始化任务对象监听的延时任务不易过多，太多会导致触发的时间误差增大，经自测每秒触发一千个回调时差在1s内。
 
