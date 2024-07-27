package delayTask_etcd

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type Option func(*etcdDelayTask)

type NewEtcdClientFunc func() (*clientv3.Client, error)

// WithEtcdTimeout：设置etcd操作的超时时间

func WithEtcdTimeout(timeout time.Duration) Option {

	return func(task *etcdDelayTask) {
		task.etcdTimeout = timeout
	}
}

// WithReConnectEtcdTimeout：设置etcd重连操作的超时时间

func WithReConnectEtcdTimeout(timeout time.Duration) Option {

	return func(task *etcdDelayTask) {
		task.reConnectTimeout = timeout
	}
}

// WithLogger：标准输出日志

func WithLogger() Option {

	return func(task *etcdDelayTask) {
		task.logger = newStdOutLog()
	}
}

//	WithEtcdHeartbeat：心跳检测重连选项,需设置NewEtcdClientFunc闭包获取新etcd连接

func WithEtcdHeartbeat(newFunc NewEtcdClientFunc) Option {

	return func(task *etcdDelayTask) {
		task.newCli = newFunc
	}
}

//	WithReConnectCallback：重连时触发的回调函数

func WithReConnectCallback(f func()) Option {

	return func(task *etcdDelayTask) {
		task.reConnectFunc = f
	}
}

// WithLoggerWrapper：自己的日志实现

func WithLoggerWrapper(log Logger) Option {

	return func(task *etcdDelayTask) {
		task.logger = log
	}
}
