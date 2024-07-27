package delayTask_etcd

type ErrMsg string

func (e ErrMsg) Error() string {
	return string(e)
}

const ClosedErr ErrMsg = "etcdDelayTask had been closed"
