package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitingTask
)

type Task struct {
	//任务种类
	TaskType TaskType
	//任务id
	TaskId int
	//任务处理的文件列表
	File []string
	// Reduce任务的数量
	NReduce int
	// Map任务的数量
	NMap int
}

// TaskStateInfo 任务状态信息
type TaskStateInfo struct {
	// 任务类型
	TaskType TaskType
	//任务开始时间用于检测超时
	StartTime time.Time
	// 任务信息
	Task *Task
	// 任务分配给的Worker ID
	WorkerId int
}

// 阻塞队列
type TaskQueue struct {
	// 任务队列，使用channel实现
	taskChan chan *TaskStateInfo
}

func NewTaskQueue(capacity int) *TaskQueue {
	return &TaskQueue{
		taskChan: make(chan *TaskStateInfo, capacity),
	}
}

func (tq *TaskQueue) offer(task *TaskStateInfo) bool {
	// 尝试将任务添加到队列中，如果队列已满则返回false
	select {
	// 如果队列未满，添加任务
	case tq.taskChan <- task:
		return true
	default:
		log.Println("TaskQueue is full, cannot add task")
		return false
	}
}

func (tq *TaskQueue) poll() *TaskStateInfo {
	// 尝试从队列中获取任务，如果没有任务则返回nil
	select {
	// 非阻塞获取任务
	case task := <-tq.taskChan:
		return task
	default:
		return nil
	}
}

func (tq *TaskQueue) blockingPoll() *TaskStateInfo {
	return <-tq.taskChan
}

type Coordinator struct {
	taskQueue          *TaskQueue             // 任务队列，使用channel实现的高效队列
	runningTasks       map[int]*TaskStateInfo // TaskId -> TaskStateInfo (正在运行的任务)
	NReduce            int                    // Reduce任务的数量
	NMap               int                    // Map任务的数量
	MapTaskFinished    int                    // 完成的Map任务数量
	ReduceTaskFinished int                    // 完成的Reduce任务数量
	AllTasksDone       bool                   // 所有任务是否完成
	mutex              sync.RWMutex           // 使用读写锁提高并发性能
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RequestTask RPC处理方法 - Worker请求任务
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//上读写锁，确保线程安全
	c.mutex.RLock()
	// 检查是否所有任务已完成
	if c.AllTasksDone {
		c.mutex.RUnlock()
		reply.TaskType = ExitingTask
		log.Printf("所有任务已完成，通知Worker %d 退出", args.WorkerId)
		return nil
	}
	c.mutex.RUnlock()

	// 1. 从队列中获取任务 (非阻塞)
	taskState := c.taskQueue.poll()

	// 2. 如果没有任务，返回等待状态
	if taskState == nil {
		reply.TaskType = WaitingTask
		return nil
	}

	// 3. 填充回复信息
	reply.TaskType = taskState.TaskType
	reply.TaskId = taskState.Task.TaskId
	reply.File = taskState.Task.File
	reply.NReduce = taskState.Task.NReduce
	reply.NMap = taskState.Task.NMap

	// 4. 记录任务分配给了哪个Worker
	taskState.WorkerId = args.WorkerId
	taskState.StartTime = time.Now()

	// 5. 将任务加入运行中的任务列表
	c.mutex.Lock()
	c.runningTasks[taskState.Task.TaskId] = taskState
	c.mutex.Unlock()

	log.Printf("分配任务给Worker %d: TaskType=%d, TaskId=%d", args.WorkerId, reply.TaskType, reply.TaskId)
	return nil
}

// FinishTask RPC处理方法 - Worker完成任务
func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	log.Printf("Worker %d完成了任务: TaskType=%d, TaskId=%d", args.WorkerId, args.TaskType, args.TaskId)

	// 1. 从运行中的任务列表中移除
	delete(c.runningTasks, args.TaskId)

	// 2. 根据任务类型处理完成逻辑
	switch args.TaskType {
	case MapTask:
		c.MapTaskFinished++
		log.Printf("Map任务 %d 已完成，Worker %d (进度: %d/%d)", args.TaskId, args.WorkerId, c.MapTaskFinished, c.NMap)

		// 检查是否所有Map任务都完成了
		if c.MapTaskFinished == c.NMap {
			log.Printf("所有Map任务已完成，开始创建Reduce任务")
			c.createReduceTasks()
		}

	case ReduceTask:
		c.ReduceTaskFinished++
		log.Printf("Reduce任务 %d 已完成，Worker %d (进度: %d/%d)", args.TaskId, args.WorkerId, c.ReduceTaskFinished, c.NReduce)

		// 检查是否所有Reduce任务都完成了
		if c.ReduceTaskFinished == c.NReduce {
			log.Printf("所有Reduce任务已完成，MapReduce作业完成！")
			c.AllTasksDone = true
		}

	default:
		log.Printf("未知任务类型完成: %d", args.TaskType)
	}

	return nil
}

// createReduceTasks 创建Reduce任务
func (c *Coordinator) createReduceTasks() {
	for i := 0; i < c.NReduce; i++ {
		task := &Task{
			TaskType: ReduceTask,
			TaskId:   i,
			File:     []string{}, // Reduce任务不需要特定的输入文件，会自动查找中间文件
			NReduce:  c.NReduce,
			NMap:     c.NMap,
		}

		taskState := &TaskStateInfo{
			TaskType: ReduceTask,
			Task:     task,
		}

		c.taskQueue.offer(taskState)
		log.Printf("创建Reduce任务 %d", i)
	}
}

// 任务检查协程
func (c *Coordinator) taskTimeoutChecker() {
	//使用定时器每10s检查一次任务状态
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		c.mutex.RLock()
		if c.AllTasksDone {
			c.mutex.RUnlock()
			return
		}
		c.mutex.RUnlock()

		<-ticker.C

		c.mutex.Lock()
		now := time.Now()
		var timeoutTasks []*TaskStateInfo

		// 检查运行中的任务是否超时（20秒）
		for taskId, taskState := range c.runningTasks {
			if now.Sub(taskState.StartTime) > 20*time.Second {
				log.Printf("任务 %d 超时，重新加入队列", taskId)
				timeoutTasks = append(timeoutTasks, taskState)
				delete(c.runningTasks, taskId)
			}
		}

		// 将超时的任务重新加入队列
		for _, taskState := range timeoutTasks {
			taskState.StartTime = time.Time{} // 重置开始时间
			taskState.WorkerId = 0            // 重置Worker ID
			c.taskQueue.offer(taskState)
		}
		c.mutex.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.AllTasksDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		taskQueue:          NewTaskQueue(1000), // 使用channel实现的高效队列
		runningTasks:       make(map[int]*TaskStateInfo),
		NReduce:            nReduce,
		NMap:               len(files),
		MapTaskFinished:    0,
		ReduceTaskFinished: 0,
		AllTasksDone:       false,
		mutex:              sync.RWMutex{},
	}

	// 1. 创建Map任务并放入队列
	for i, file := range files {
		task := &Task{
			TaskType: MapTask,
			TaskId:   i,
			File:     []string{file}, // Map任务只处理一个文件
			NReduce:  nReduce,
			NMap:     len(files),
		}

		taskState := &TaskStateInfo{
			TaskType: MapTask,
			Task:     task,
		}

		c.taskQueue.offer(taskState)
		log.Printf("创建Map任务 %d: %s", i, file)
	}

	log.Printf("Coordinator初始化完成: %d个Map任务, %d个Reduce任务", len(files), nReduce)

	// 启动任务超时检查协程
	go c.taskTimeoutChecker()

	c.server()
	return c
}
