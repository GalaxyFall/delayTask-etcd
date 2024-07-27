package delayTask_etcd

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync"
	"time"
)

var _ DelayTask = (*etcdDelayTask)(nil)

type DelayTask interface {

	/*
			SetDelayTask：开始一个延时任务 延时时间到达异步触发设置的回调函数
		                  该设置为覆盖设置，将会重新租约并且替换回调
	*/
	SetDelayTask(key string, f func(), ttl int64) error

	/*
		CancelDelayTask ：取消延时任务
	*/
	CancelDelayTask(key string) error

	/*
		Close： 取消监听延时任务退出
	*/
	Close() error
}

type etcdDelayTask struct {
	mu sync.RWMutex

	logger           Logger //标准输出日志
	prefix           string //监听前缀key
	cli              *clientv3.Client
	newCli           NewEtcdClientFunc //创建一个etcd新连接的闭包  心跳检测断线
	etcdTimeout      time.Duration     //操作etcd的超时时间
	reConnectTimeout time.Duration     //重连etcd的超时时间
	keyFns           map[string]func() //回调函数映射
	exit             chan struct{}
	closed           bool
	reConnectFunc    func() //重连触发的回调函数
}

/*   创建延时任务对象,开始监听前缀key    */

func NewEtcdDelayTask(cli *clientv3.Client, prefix string, opts ...Option) (DelayTask, error) {
	if cli == nil {
		return nil, errors.New("etcd client nil")
	}

	task := &etcdDelayTask{
		mu:               sync.RWMutex{},
		prefix:           prefix,
		cli:              cli,
		etcdTimeout:      time.Second * 5, //默认5秒
		reConnectTimeout: time.Second * 3, //默认3秒
		keyFns:           map[string]func(){},
		exit:             make(chan struct{}, 1),
	}

	for _, opt := range opts {
		opt(task)
	}

	go task.startWatch()

	return task, nil
}

func (e *etcdDelayTask) SetDelayTask(k string, f func(), ttl int64) error {
	if f == nil {
		return errors.New("callback func empty")
	}

	if e.closed {
		return ClosedErr
	}

	key := fmt.Sprintf("%s/%s", e.prefix, k)

	e.setCallback(key, f)

	ctx, cancel := context.WithTimeout(context.TODO(), e.etcdTimeout)
	defer cancel()

	lease, err := e.cli.Grant(ctx, ttl)
	if err != nil {
		return err
	}

	//监听DELETE事件不会收到监听key的val 这里忽略
	_, err = e.cli.Put(ctx, key, "", clientv3.WithLease(lease.ID))
	return err
}

func (e *etcdDelayTask) CancelDelayTask(k string) error {
	if e.closed {
		return ClosedErr
	}

	key := fmt.Sprintf("%s/%s", e.prefix, k)

	//不存在则正常返回
	ok := e.delCallback(key)
	if !ok {
		return nil
	}

	//尝试去删除key
	ctx, cancel := context.WithTimeout(context.TODO(), e.etcdTimeout)
	defer cancel()

	resp, err := e.cli.Get(ctx, key)
	//if key expired
	if err != nil {
		return nil
	}

	if len(resp.Kvs) != 0 {
		ctx, cancel := context.WithTimeout(context.TODO(), e.etcdTimeout)
		defer cancel()
		_, err = e.cli.Revoke(ctx, clientv3.LeaseID(resp.Kvs[0].Lease))
		if err != nil {
			return err
		}

		_, err = e.cli.Delete(ctx, key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *etcdDelayTask) Close() error {

	if e.closed {
		return ClosedErr
	}

	ctx, cancel := context.WithTimeout(context.TODO(), e.etcdTimeout)
	defer cancel()

	//先发送关闭信号
	e.exit <- struct{}{}

	resp, err := e.cli.Get(ctx, e.prefix, clientv3.WithPrefix())
	//if key expired
	if err != nil {
		return nil
	}

	//revoke lease and delete key
	for _, kv := range resp.Kvs {
		ctx, cancel := context.WithTimeout(context.TODO(), e.etcdTimeout)
		_, err = e.cli.Revoke(ctx, clientv3.LeaseID(kv.Lease))
		if err != nil {
			cancel()
			return err
		}

		_, err = e.cli.Delete(ctx, e.prefix, clientv3.WithPrefix())
		if err != nil {
			cancel()
			return err
		}
		cancel()
	}

	return nil
}

func (e *etcdDelayTask) startWatch() {
	e.logf("delay task prefix %s start watching ...", e.prefix)

	defer func() {
		e.closed = true //设置已经退出
	}()

	tick := time.NewTicker(time.Second)
	defer tick.Stop()

initWatch:
	for {
		//判断是否已经需要退出
		ctx, cancel := context.WithCancel(context.TODO())

		watchChan := e.cli.Watch(ctx, e.prefix, clientv3.WithPrefix())
		for {
			select {
			case <-e.exit:
				cancel()
				e.logf("watch prefix %s receive close chanel ...", e.prefix)
				return

			case <-tick.C:
				//没有心跳选项则不处理
				if e.newCli == nil {
					continue
				}
				//先判断etcd连接是否有效再选择触发重连
				if !e.isEtcdConnect() {
					e.logf("etcd disconnect and start reconnect ... ")
					if err := e.reConnectEtcd(); err != nil {
						e.errorf("etcd try reconnect err")
						continue
					}
					e.logf("etcd reconnect success...")
					cancel()
					continue initWatch
				}
				e.logf("etcd heartBeat option working ...")

			case c, ok := <-watchChan:
				if !ok {
					e.errorf("watch key %s receive chanel had been closed ...", e.prefix)
					cancel()
					continue initWatch
				}

				if c.Err() != nil {
					e.errorf("watch prefix %s failed, retrying...", e.prefix)
					cancel()
					time.Sleep(1 * time.Second) // 退避策略
					continue initWatch
				}

				if c.Canceled {
					e.errorf("watch key %s receive chanel had been closed ...", e.prefix)
					cancel()
					return
				}

				for _, event := range c.Events {
					if event.Type == mvccpb.DELETE {
						e.logf("watch key %s receive event", event.Kv.Key)
						if f, ok := e.getCallback(string(event.Kv.Key)); ok {
							e.logf("watch key %s receive event exec fallback func ...", event.Kv.Key)
							go f()
						}
					}
				}
			}
		}
	}

}

func (e *etcdDelayTask) setCallback(key string, f func()) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.keyFns[key] = f
}

// 如果key不存在返回false
func (e *etcdDelayTask) delCallback(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	_, ok := e.keyFns[key]
	if !ok {
		return false
	}

	delete(e.keyFns, key)

	return true
}

func (e *etcdDelayTask) getCallback(key string) (func(), bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	f, ok := e.keyFns[key]

	return f, ok
}

func (e *etcdDelayTask) isEtcdConnect() bool {
	ctx, cancel := context.WithTimeout(context.TODO(), e.reConnectTimeout)
	defer cancel()

	_, err := e.cli.MemberList(ctx)
	if err != nil {
		return false
	}

	return true
}

func (e *etcdDelayTask) reConnectEtcd() error {
	//尝试关闭旧连接
	_ = e.cli.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), e.reConnectTimeout)
	defer cancel()

	f := e.newCli
	if f == nil {
		return errors.New("new etcd func empty")
	}

	newCli, err := f()
	if err != nil {
		return err
	}

	_, err = newCli.MemberList(ctx)
	if err != nil {
		return err
	}

	//重连成功
	e.cli = newCli

	if e.reConnectFunc != nil {
		go e.reConnectFunc()
	}

	return nil
}

func (e *etcdDelayTask) logf(format string, v ...interface{}) {
	if e.logger == nil {
		return
	}
	e.logger.Info(format, v...)
}

func (e *etcdDelayTask) errorf(format string, v ...interface{}) {
	if e.logger == nil {
		return
	}
	e.logger.Error(format, v...)
}
