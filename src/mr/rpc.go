package mr

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// Worker 请求任务（核心 RPC）
// 因为 Worker 问 Coordinator 要任务，不需要带任何参数！
type TaskRequest struct{}

type TaskResponse struct {
	TaskType TaskType // 任务类型：Map/Reduce/Wait/Exit
	TaskId   int      // 任务编号（唯一标识）
	File     string   // 要处理的文件名（Map 用）
	NMap     int      // 总共有多少个 Map 任务
	NReduce  int      // 总共有多少个 Reduce 任务
}

// Worker 完成任务后汇报
// 我完成了 类型是 XX、编号是 XX 的任务！
type TaskDoneRequest struct {
	TaskType TaskType // 完成的是什么任务
	TaskId   int      // 完成的任务编号
}

// 完成响应（空结构体）
type TaskDoneResponse struct{}

//Coordinator 回复：收到，知道你做完了
