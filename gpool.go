/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2019/12/27
   Description :
-------------------------------------------------
*/

package zgpool

import (
	"sync"
)

// 任务
type Job func()

// 工人
type worker struct {
	jobChannel chan Job      // 工作任务
	stop       chan struct{} // 停止信号
}

// 准备好
func (w *worker) Ready() {
	go func() {
		var job Job
		for {
			select {
			case job = <-w.jobChannel: // 等待任务
				job()
			case <-w.stop:
				w.stop <- struct{}{}
				return
			}
		}
	}()
}

// 做任务
func (w *worker) Do(job Job) {
	w.jobChannel <- job
}

// 停止
func (w *worker) Stop() {
	w.stop <- struct{}{}
	<-w.stop
}

// 创建一个工人
func newWorker(pool chan *worker) *worker {
	return &worker{
		jobChannel: make(chan Job),
		stop:       make(chan struct{}),
	}
}

// 协程池
type Pool struct {
	workerQueue chan *worker // 工人队列
	jobQueue    chan Job     // 任务队列
	stop        chan struct{}

	wg sync.WaitGroup
}

// 创建协程池
// numWorkers表示工人数, jobQueueLen表示任务数, 如果任务队列已满还添加则会等待任务队列出现空缺
func NewPool(numWorkers int, jobQueueLen int) *Pool {
	pool := &Pool{
		workerQueue: make(chan *worker, numWorkers),
		jobQueue:    make(chan Job, jobQueueLen),
		stop:        make(chan struct{}),
	}
	return pool
}

// 起飞
func (p *Pool) Start() {
	for i := 0; i < cap(p.workerQueue); i++ {
		worker := newWorker(p.workerQueue)
		worker.Ready()
		p.workerQueue <- worker
	}

	go p.dispatch()
}

// 添加任务
func (p *Pool) Go(job Job) {
	p.wg.Add(1)
	p.jobQueue <- job
}

// 为工人派遣任务
func (p *Pool) dispatch() {
	for {
		select {
		case job := <-p.jobQueue:
			p.solve(job)
		case <-p.stop:
			for i := 0; i < cap(p.workerQueue); i++ {
				worker := <-p.workerQueue
				worker.Stop()
			}

			p.stop <- struct{}{}
			return
		}
	}
}

// 解决任务
func (p *Pool) solve(job Job) {
	worker := <-p.workerQueue
	worker.Do(func() {
		job()
		p.wg.Done()
		p.workerQueue <- worker
	})
}

// 停止
// 命令所有没有收到任务的工人立即停工, 收到任务的工人完成当前任务后停工, 不管任务队列是否清空
func (p *Pool) Stop() {
	p.stop <- struct{}{}
	<-p.stop
}

// 等待所有任务结束
func (p *Pool) Wait() {
	p.wg.Wait()
}
