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

type Coordinator struct {
	mu          sync.Mutex // 互斥锁（并发安全！）多个 Worker 同时问老板要任务
	mapTasks    []Task     // 所有 Map 任务
	reduceTasks []Task     // 所有 Reduce 任务
	nMap        int        // 总共有多少个 Map 任务
	nReduce     int        // 总共有多少个 Reduce 任务
	mapDone     int        // 已完成的 Map 数量
	reduceDone  int        // 已完成的 Reduce 数量
}
type Task struct {
	id        int        // 任务唯一编号
	file      string     // 要处理的文件名
	status    TaskStatus // 任务状态（待执行/执行中/已完成）
	startTime time.Time  // 任务开始时间（用于超时重试）
}
type TaskStatus int

// 任务状态：空闲 / 执行中 / 已完成
const (
	Idle       TaskStatus = iota // 0 空闲，可分配
	Processing                   // 1 执行中
	Done                         // 2 已完成
)

// 任务超时：10秒没完成，认为Worker挂了，重新分配
const Timeout = 10 * time.Second

// GetTask：worker 调用，获取任务
// args：Worker 发来的请求（空的，就是来要任务）
// reply：Coordinator 给 Worker 的回复（做什么任务、文件、编号等）s
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 遍历所有Map任务，找一个空闲的分配
	for i := range c.mapTasks {
		t := &c.mapTasks[i] // 拿到任务指针（直接修改原任务）

		if t.status == Idle { // 找到空闲任务！
			t.status = Processing    // 标记为执行中
			t.startTime = time.Now() // 记录开始时间（超时重试用）

			// 给Worker回复任务信息
			reply.TaskType = MapTask
			reply.TaskId = t.id
			reply.File = t.file
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce

			return nil // 分配成功，直接返回
		}
	}

	// 检查：有没有Map任务还没完成？
	for i := range c.mapTasks {
		if c.mapTasks[i].status != Done {
			reply.TaskType = WaitTask // 告诉Worker：等着！
			return nil
		}
	}

	// 遍历所有Reduce任务，找空闲的
	for i := range c.reduceTasks {
		t := &c.reduceTasks[i]
		if t.status == Idle {
			t.status = Processing
			t.startTime = time.Now()

			// 回复Reduce任务
			reply.TaskType = ReduceTask
			reply.TaskId = t.id
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce

			return nil
		}
	}
	// 只有【所有任务都真正完成】才退出
	if c.mapDone == c.nMap && c.reduceDone == c.nReduce {
		reply.TaskType = ExitTask
	} else {
		// 否则让 Worker 等待
		reply.TaskType = WaitTask
	}
	return nil
}

// TaskDone：worker 完成任务后上报
// args：Worker 传来的参数（任务类型 + 任务ID）
// reply：给 Worker 的返回（一般空的）
func (c *Coordinator) TaskDone(args *TaskDoneRequest, reply *TaskDoneResponse) error {
	// 1. 上锁！并发安全，防止多个Worker同时改状态
	c.mu.Lock()
	defer c.mu.Unlock()

	// 2. 如果是 Map 任务完成
	if args.TaskType == MapTask {
		// 只有任务不是 Done 状态，才更新（防止重复上报）
		if c.mapTasks[args.TaskId].status != Done {
			c.mapTasks[args.TaskId].status = Done // 标记任务完成
			c.mapDone++                           // 已完成 Map 数量 +1
		}
	} else if args.TaskType == ReduceTask { // 3. 如果是 Reduce 任务完成
		// 同样防止重复上报
		if c.reduceTasks[args.TaskId].status != Done {
			c.reduceTasks[args.TaskId].status = Done // 标记完成
			c.reduceDone++                           // 已完成 Reduce 数量 +1
		}
	}

	return nil // 没有错误
}

// 超时检查协程
// 如果某个任务执行超过 10 秒没完成，就认为 Worker 挂了，
// 把任务重置为空闲，让别人重新执行。
// checkTimeout：后台无限循环，检查超时任务
// 必须用 go c.checkTimeout() 启动！
func (c *Coordinator) checkTimeout() {
	// 无限循环：一直检查
	for {
		// 每秒检查一次（不要太频繁）
		time.Sleep(1 * time.Second)

		// 上锁：修改任务状态，必须保证并发安全
		c.mu.Lock()

		// 1. 检查所有 Map 任务是否超时
		for i := range c.mapTasks {
			t := &c.mapTasks[i]
			// 如果任务正在执行，且开始时间距离现在超过10秒 → 超时！
			if t.status == Processing && time.Since(t.startTime) > Timeout {
				t.status = Idle // 重置为空闲，让其他Worker重新领取
			}
		}

		// 2. 检查所有 Reduce 任务是否超时
		for i := range c.reduceTasks {
			t := &c.reduceTasks[i]
			if t.status == Processing && time.Since(t.startTime) > Timeout {
				t.status = Idle // 重置任务
			}
		}

		// 解锁
		c.mu.Unlock()
	}
}

// server：启动 Coordinator 的 RPC 服务
// sockname：Unix 套接字文件路径（如 /tmp/mr-sock）
func (c *Coordinator) server(sockname string) {
	// 1. 把当前 Coordinator 对象注册为 RPC 服务
	// 告诉系统：GetTask / TaskDone 这两个方法可以被远程调用
	rpc.Register(c)

	// 2. 把 RPC 服务绑定到 HTTP 协议上
	// Go 的 RPC 底层基于 HTTP 传输
	rpc.HandleHTTP()

	// 3. 先删除旧的套接字文件（防止上次异常退出残留）
	// Unix 套接字是文件，程序崩溃不会自动删除
	os.Remove(sockname)

	// 4. 监听 Unix 域套接字（本地进程间高速通信）
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}

	// 5. 启动 HTTP 服务（后台协程，不阻塞主线程）
	go http.Serve(l, nil)
}

// Done：判断整个 MapReduce 任务是否全部完成
// 主线程会一直调用这个函数，直到返回 true
func (c *Coordinator) Done() bool {
	// 上锁：读取 mapDone 和 reduceDone，保证数据安全
	c.mu.Lock()
	// 函数退出自动解锁
	defer c.mu.Unlock()

	// 核心判断：所有 Map 做完 + 所有 Reduce 做完
	return c.mapDone == c.nMap && c.reduceDone == c.nReduce
}

// MakeCoordinator：创建并初始化一个 Coordinator
// 传入：套接字名、输入文件列表、需要多少个 Reduce 任务
// 返回：创建好的 Coordinator 指针
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	// 1. 创建 Coordinator 实例，初始化基础字段
	c := Coordinator{
		nMap:    len(files), // Map 任务数 = 输入文件数
		nReduce: nReduce,    // Reduce 任务数 = 用户指定
	}

	// 2. 初始化所有 Map 任务
	c.mapTasks = make([]Task, c.nMap) // 创建 nMap 个 Task
	for i, f := range files {
		c.mapTasks[i] = Task{
			id:     i,    // 任务编号 0,1,2...
			file:   f,    // 对应的输入文件
			status: Idle, // 初始状态：空闲
		}
	}

	// 3. 初始化所有 Reduce 任务
	c.reduceTasks = make([]Task, nReduce) // 创建 nReduce 个 Task
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			id:     i,    // 编号 0,1,2...
			status: Idle, // 初始状态：空闲
		}
	}

	// 4. 启动后台超时检查协程
	go c.checkTimeout()

	// 5. 启动 RPC 服务，让 Worker 能连接
	c.server(sockname)

	// 6. 返回创建好的 Coordinator
	return &c
}
