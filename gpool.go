/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2019/12/27
   Description :
-------------------------------------------------
*/

package zgpool

// 任务
type Job func()

// 工人
type worker struct {
	workerQueue chan *worker  // 主工人队列
	jobChannel  chan Job      // 工作任务
	stop        chan struct{} // 停止信号
}

// 开始工作
func (w *worker) Start() {
	go func() {
		var job Job
		for {
			w.workerQueue <- w // 重新加入队列
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

// 创建一个工人
func newWorker(pool chan *worker) *worker {
	return &worker{
		workerQueue: pool,
		jobChannel:  make(chan Job),
		stop:        make(chan struct{}),
	}
}

// 协程池
type Pool struct {
	workerQueue chan *worker // 工人队列
	jobQueue    chan Job     // 任务队列
	stop        chan struct{}
}

// 创建协程池
// numWorkers表示工人数, jobQueueLen表示任务数, 如果任务队列已满还添加则会等待任务队列出现空缺
func NewPool(numWorkers int, jobQueueLen int) *Pool {
	workerQueue := make(chan *worker, numWorkers)
	jobQueue := make(chan Job, jobQueueLen)

	pool := &Pool{
		workerQueue: workerQueue,
		jobQueue:    jobQueue,
		stop:        make(chan struct{}),
	}
	return pool
}

// 起飞
func (p *Pool) Start() {
	for i := 0; i < cap(p.workerQueue); i++ {
		worker := newWorker(p.workerQueue)
		worker.Start()
	}

	go p.dispatch()
}

// 添加任务
func (p *Pool) Go(job Job) {
	p.jobQueue <- job
}

// 为工人派遣任务
func (p *Pool) dispatch() {
	for {
		select {
		case job := <-p.jobQueue:
			worker := <-p.workerQueue
			worker.jobChannel <- job
		case <-p.stop:
			for i := 0; i < cap(p.workerQueue); i++ {
				worker := <-p.workerQueue

				worker.stop <- struct{}{}
				<-worker.stop
			}

			p.stop <- struct{}{}
			return
		}
	}
}

// 停止
// 命令所有没有收到任务的工人立即停工, 收到任务的工人完成当前任务后停工, 不管任务队列是否清空
func (p *Pool) Stop() {
	p.stop <- struct{}{}
	<-p.stop
}
