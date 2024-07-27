package delayTask_etcd

import (
	"context"
	"errors"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

const testPrefix = "/test"
const testKey = "hello"

const endpoint string = "127.0.0.1:2379"

func etcdInit() (*clientv3.Client, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		return nil, errors.New("open etcd failed...")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	_, err = client.Status(ctx, endpoint)
	if err != nil {
		return nil, errors.New("etcd connect failed...")
	}

	fmt.Println("etcd connect success,endpoint: ", endpoint)

	return client, nil
}

func TestEtcdDelayTask_SetDelayTask(t *testing.T) {

	t.Log("test set delay task start ...")

	cli, err := etcdInit()
	if err != nil {
		t.Fatal(err.Error())
	}

	task, err := NewEtcdDelayTask(cli, testPrefix, WithLogger())
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer task.Close()

	var testCount int

	fn := func() {
		testCount++
		t.Logf("i am worker,change testCount to %d", testCount)
	}

	if err := task.SetDelayTask(testKey, fn, 5); err != nil {
		t.Fatal(err)
	}

	timer := time.NewTimer(time.Second * 6)
	defer timer.Stop()

	select {
	case <-timer.C:
		if testCount == 0 {
			t.Fatal("test set delay task fail")
		}
	}

}

func TestEtcdDelayTask_RepetitionSetDelayTask(t *testing.T) {

	t.Log("test repetition set delay task start ...")

	cli, err := etcdInit()
	if err != nil {
		t.Fatal(err.Error())
	}

	task, err := NewEtcdDelayTask(cli, testPrefix, WithLogger())
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer task.Close()

	var testCount int
	var startTime time.Time
	testBeforefn := func() {
		testCount++
		t.Logf("i am worker,change testCount to %d", testCount)
	}

	//测试存在未执行的延时任务时再设置延时任务 是否有重复执行 执行时间以为后一次设置时间
	var delay int64 = 10

	testTimefn := func() {
		receiveTime := time.Now().Unix()
		gap := calcuDiffVal(receiveTime, startTime.Unix()+delay)

		//这里正常误差在1秒内即可
		t.Logf("receive test delay task key %s delay %d time %d gap %d", testKey, delay, receiveTime, gap)
		if gap > 1 {
			t.Fatalf("delay task error over 1s")
		}
	}

	t.Log("test repetition set first delay task")
	if err := task.SetDelayTask(testKey, testBeforefn, delay); err != nil {
		t.Fatal(err)
	}

	// 2-9
	time.Sleep(time.Second * time.Duration(rand.Int63n(delay-3)+2))

	t.Log("test repetition set repetition delay task")
	startTime = time.Now()
	if err := task.SetDelayTask(testKey, testTimefn, delay); err != nil {
		t.Fatal(err)
	}

	timer := time.NewTimer(time.Second*time.Duration(delay) + time.Second*2)
	defer timer.Stop()

	select {
	case <-timer.C:
		if testCount != 0 {
			t.Fatal("test repetition set delay task fail")
		}
	}

}

func TestEtcdDelayTask_BatchSetDelayTask(t *testing.T) {

	const count int64 = 5000
	var execCount int64

	t.Log("test batch set delay task start ...")

	cli, err := etcdInit()
	if err != nil {
		t.Fatal(err)
	}

	task, err := NewEtcdDelayTask(cli, testPrefix)
	if err != nil {
		t.Fatal(err)
	}

	//为什么延时一个极短的时间通常失败？ 如 0 误差通常 2s

	var delay int64 = 10

	for i := 0; i < int(count); i++ {
		go func(i int) {
			// 随机 2 到 delay-3
			r := rand.Int63n(delay-3) + 2
			testDelay(strconv.Itoa(i), r, task, &execCount, t)
		}(i)
	}

	//阻塞
	timer := time.NewTimer(time.Second * time.Duration(delay+10))
	defer timer.Stop()

	select {
	case <-timer.C:
		if execCount != count {
			t.Fatalf("test batch set delay task fail, exec count %d", execCount)
		}
		t.Logf("test batch set delay task success, exec count %d", execCount)
	}

}

func TestEtcdDelayTask_CancelDelayTask(t *testing.T) {

	t.Log("test cancel delay task start ...")

	cli, err := etcdInit()
	if err != nil {
		t.Fatal(err)
	}

	task, err := NewEtcdDelayTask(cli, testPrefix, WithLogger())
	if err != nil {
		t.Fatal(err)
	}

	var testCount int

	fn := func() {
		testCount++
		t.Logf("i am worker,change testCount to %d", testCount)
	}

	if err := task.SetDelayTask(testKey, fn, 5); err != nil {
		t.Fatal(err)
	}

	//取消任务
	if err := task.CancelDelayTask(testKey); err != nil {
		t.Fatal(err)
	}

	//测试设置的key是否还在
	exist, err := isKeyExist(fmt.Sprintf("%s/%s", testPrefix, testKey), cli)
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Fatal("test cancel delay task fail key exist")
	}

	//重复取消任务
	if err := task.CancelDelayTask(testKey); err != nil {
		t.Fatal("test repetition close delay task fail err", err.Error())
	}

	timer := time.NewTimer(time.Second * 6)
	defer timer.Stop()

	select {
	case <-timer.C:
		if testCount != 0 {
			t.Fatal("test cancel delay task fail")
		}
	}
}

func TestEtcdDelayTask_Close(t *testing.T) {

	t.Log("test close delay task start ...")

	cli, err := etcdInit()
	if err != nil {
		t.Fatal(err)
	}

	task, err := NewEtcdDelayTask(cli, testPrefix)
	if err != nil {
		t.Fatal(err)
	}

	//正常关闭
	if err := task.Close(); err != nil {
		t.Fatal("test normal close delay task fail err", err.Error())
	}

	//重复关闭
	if err := task.Close(); err != nil && !errors.Is(err, ClosedErr) {
		t.Fatal("test repetition close delay task fail err", err.Error())
	}

}

func TestEtcdDelayTask_reConn(t *testing.T) {

	t.Log("test delay task reconnect start ...")

	cli, err := etcdInit()
	if err != nil {
		t.Fatal(err)
	}

	//重新连接会刷新etcd租约时间  这里使用闭包获取重连的时间 fns会在etcd重连执行
	var reConnectTime time.Time

	fns := func() {
		printfKeyLeasTimeFunc()
		reConnectTime = time.Now()

	}

	task, err := NewEtcdDelayTask(cli, testPrefix,
		WithEtcdHeartbeat(etcdInit),
		WithReConnectCallback(fns),
		WithReConnectEtcdTimeout(time.Second*2),
		WithLogger())
	if err != nil {
		t.Fatal(err)
	}

	var testCount int

	fn := func() {
		receiveTime := time.Now().Unix()
		testCount++
		t.Logf("i am worker,change testCount to %d,time %d", testCount, receiveTime)
		//误差允许2s reConnectTime可能不太准确
		if receiveTime-(reConnectTime.Unix()+5) > 2 {
			t.Fatalf("reconnect delay task error over 2s")
		}
	}

	//重连前开始一个延时任务  、
	t.Logf("test delay task reconnect set dalay task time %d", time.Now().Unix())
	if err := task.SetDelayTask(testKey, fn, 5); err != nil {
		t.Fatalf(err.Error())
	}

	t.Logf("test delay etcd disconnect %d", time.Now().Unix())
	//这里的时间段操作：看到打印了关闭etcd服务  打印检测断开之后立即重新连接

	timer := time.NewTimer(time.Second * 40)
	defer timer.Stop()

	select {
	case <-timer.C:
		if testCount == 0 {
			t.Fatal("test delay task reconnect fail...")
		}
	}

	t.Logf("test delay task reconnect success")
}

func TestEtcdDelayTask_reConnSetDelayTask(t *testing.T) {

	t.Log("test delay task reconnect after set delay task start ...")

	cli, err := etcdInit()
	if err != nil {
		t.Fatal(err)
	}

	var testCount int
	var reConnectTime time.Time
	var task DelayTask

	//这里闭包测试重连etcd的延时任务是否符合预期
	fn := func() {
		receiveTime := time.Now().Unix()
		testCount++
		t.Logf("i am worker,change testCount to %d,time %d", testCount, receiveTime)
		//误差允许2s 取reConnectTime可能不太准确与etcd的重连时间也有误差
		if receiveTime-(reConnectTime.Unix()+5) > 2 {
			t.Fatalf("reconnect delay task error over 2s")
		}
	}

	//重新连接会刷新etcd租约时间  这里使用闭包获取重连的时间 fns会在etcd重连执行

	fns := func() {
		reConnectTime = time.Now()
		//重连后开始一个延时任务  、
		t.Logf("test delay task reconnect set dalay task time %d", time.Now().Unix())
		if err := task.SetDelayTask(testKey, fn, 5); err != nil {
			t.Fatalf(err.Error())
		}
	}

	task, err = NewEtcdDelayTask(
		cli,
		testPrefix,
		WithEtcdHeartbeat(etcdInit),
		WithReConnectCallback(fns),
		WithLogger())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("test delay etcd disconnect %d", time.Now().Unix())
	//这里的时间段操作：看到打印了关闭etcd服务  打印检测断开之后立即重新连接

	timer := time.NewTimer(time.Second * 40)
	defer timer.Stop()

	select {
	case <-timer.C:
		if testCount == 0 {
			t.Fatal("test delay task reconnect fail...")
		}
	}

	t.Logf("test delay task reconnect success")
}

func TestEtcdDelayTask_reConnCancelDelayTask(t *testing.T) {

	t.Log("test delay task reconnect after cancel delay task start ...")

	cli, err := etcdInit()
	if err != nil {
		t.Fatal(err)
	}

	var testCount int
	var task DelayTask

	//这里闭包测试重连etcd的延时任务是否符合预期
	fn := func() {
		receiveTime := time.Now().Unix()
		testCount++
		t.Logf("i am worker,change testCount to %d,time %d", testCount, receiveTime)
	}

	//重新连接会刷新etcd租约时间  这里使用闭包获取重连的时间 fns会在etcd重连执行

	fns := func() {
		//重连后开始一个延时任务 测试再取消 、
		t.Logf("test delay task reconnect after cancel dalay task time %d", time.Now().Unix())
		if err := task.SetDelayTask(testKey, fn, 5); err != nil {
			t.Fatalf(err.Error())
		}

		if err := task.CancelDelayTask(testKey); err != nil {
			t.Fatal(err)
		}
	}

	task, err = NewEtcdDelayTask(
		cli,
		testPrefix,
		WithEtcdHeartbeat(etcdInit),
		WithReConnectCallback(fns),
		WithLogger())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("test delay etcd disconnect %d", time.Now().Unix())
	//这里的时间段操作：看到打印了关闭etcd服务  打印检测断开之后立即重新连接

	timer := time.NewTimer(time.Second * 40)
	defer timer.Stop()

	select {
	case <-timer.C:
		if testCount != 0 {
			t.Fatal("test delay task reconnect after cancel delay task fail...")
		}
	}

	t.Logf("test delay task reconnect after cancel delay task success")
}

func testDelay(key string, delay int64, task DelayTask, count *int64, t *testing.T) {

	startTime := time.Now().Unix()
	t.Logf("start test delay task key %s delay %d time %d", key, delay, startTime)

	f := func() {
		receiveTime := time.Now().Unix()
		gap := calcuDiffVal(receiveTime, startTime+delay)

		//统计一下执行了延时任务的数量是否符合预期
		atomic.AddInt64(count, 1)

		//这里正常误差在1秒内即可
		t.Logf("receive test delay task key %s delay %d time %d gap %d", key, delay, receiveTime, gap)
		if gap > 1 {
			t.Fatalf("delay task error over 1s")
		}
	}

	if err := task.SetDelayTask(key, f, delay); err != nil {
		t.Fatalf("SetDelayTask err %s", err.Error())
	}

}

func calcuDiffVal(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return b - a
}

func printfKeyLeasTimeFunc() {

	fmt.Println("printfKeyLeasTimeFunc ...")
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()

	cli, err := etcdInit()
	if err != nil {
		fmt.Println("printfKeyLeasTimeFunc etcdInit err")
		return
	}

	resp, err := cli.Get(ctx, testPrefix, clientv3.WithPrefix())
	//if key expired
	if err != nil {
		fmt.Println("printfKeyLeasTimeFunc Get err")
		return
	}

	//revoke lease and delete key
	for _, kv := range resp.Kvs {
		ret, err := cli.TimeToLive(ctx, clientv3.LeaseID(kv.Lease))
		if err != nil {
			fmt.Println("printfKeyLeasTimeFunc TimeToLive err")
			return
		}

		fmt.Printf("key %s Lease TTL: %d, GrantedTTL: %d  time %d \n", string(kv.Key), ret.TTL, ret.GrantedTTL, time.Now().Unix())
	}

}

func isKeyExist(key string, cli *clientv3.Client) (bool, error) {
	fmt.Println("isKeyExist func")

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()

	cli, err := etcdInit()
	if err != nil {
		return false, err
	}

	resp, err := cli.Get(ctx, testPrefix, clientv3.WithPrefix())
	//if key expired
	if err != nil {
		return false, nil
	}

	for _, kv := range resp.Kvs {
		ret, err := cli.TimeToLive(ctx, clientv3.LeaseID(kv.Lease))
		if err != nil {
			fmt.Println("isKeyExist TimeToLive err")
			return false, err
		}

		fmt.Printf("isKeyExist key %s Lease TTL: %d, GrantedTTL: %d  time %d \n", string(kv.Key), ret.TTL, ret.GrantedTTL, time.Now().Unix())
		if key == string(kv.Key) {
			return true, nil
		}
	}
	return false, nil
}
