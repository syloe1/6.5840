package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string // 单词（如 "hello"）
	Value string // 计数（如 "1"）
}

// 把 Key 哈希成数字 → 决定发给哪个 Reduce 任务
func ihash(key string) int {
	h := fnv.New32a() //fnv快速哈希算法
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string //全局字符串， 存Coordinator地址

// socket 名称（通信地址）, 函数返回kv， 函数返回string
// 无限循环：
//
//	请求任务 → 接收任务
//	如果是 Map → 执行 Map → 汇报完成
//	如果是 Reduce → 执行 Reduce → 汇报完成
//	如果是 Wait → 睡一秒
//	如果是 Exit → 退出
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname // 保存 Coordinator 地址

	// 死循环：不停要任务
	for {
		// 1. 构造请求：老板给我任务
		args := TaskRequest{}
		reply := TaskResponse{}

		// 2. 发 RPC 请求
		if !call("Coordinator.GetTask", &args, &reply) {
			return // 连接失败，退出
		}

		// 3. 根据任务类型执行
		switch reply.TaskType {
		case MapTask:
			doMap(reply.TaskId, reply.File, reply.NReduce, mapf)
			reportDone(MapTask, reply.TaskId) // 做完汇报

		case ReduceTask:
			doReduce(reply.TaskId, reply.NMap, reducef)
			reportDone(ReduceTask, reply.TaskId)

		case WaitTask:
			time.Sleep(1 * time.Second) // 没任务，等1秒再问

		case ExitTask:
			return // 全部结束，退出
		}
	}
}

func reportDone(t TaskType, id int) {
	req := TaskDoneRequest{TaskType: t, TaskId: id}
	rsp := TaskDoneResponse{}
	//调用Coordinator的TaskDone函数把 我的完成请求发过去
	call("Coordinator.TaskDone", &req, &rsp)
}

// 读取输入文件 → 调用你写的 map 逻辑 → 生成中间结果文件
// \func doMap(
//
//	mapId int,        // 第几个 Map 任务（编号）
//	inFile string,    // 要处理的**输入文件名**
//	nReduce int,      // 一共有多少个 Reduce 任务
//	mapf func(string, string) []KeyValue  // 你自己写的 Map 处理逻辑
//
// )
// ：读文件 → 调用 mapf 生成键值对 → 按 key 哈希分成 nReduce 份 → 写入中间文件（mr-X-Y）
func doMap(mapId int, inFile string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(inFile)
	if err != nil {
		log.Fatalf("cannot open %v", inFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inFile)
	}
	file.Close()
	// 3. 调用你写的 mapf 函数！
	// 传入：文件名 + 文件内容
	// 输出：一堆键值对，例如 ["hello":1, "world":1]
	kva := mapf(inFile, string(content))
	// 4. 创建 nReduce 个“桶”
	// 比如 nReduce=3，就创建 3 个列表
	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		intermediate[r] = append(intermediate[r], kv)
	}
	// 6. 把每个桶的结果，写入中间文件
	for r := 0; r < nReduce; r++ {
		// 文件名格式：mr-任务号-桶号 → 例如 mr-0-0、mr-0-1、mr-0-2
		oname := fmt.Sprintf("mr-%d-%d", mapId, r)
		tmp, _ := os.CreateTemp(".", "tmp-*")
		//JSON格式把键值对写进去
		enc := json.NewEncoder(tmp)
		for _, kv := range intermediate[r] {
			enc.Encode(&kv) //写成json存文件
		}
		tmp.Close()
		os.Rename(tmp.Name(), oname) //写完改名，保证原子性
	}
}

// 执行 Reduce 任务
func doReduce(
	reduceId int, // 当前是第几个 Reduce 任务
	nMap int, // 总共有多少个 Map 任务
	reducef func(string, []string) string, // 你写的 Reduce 逻辑
) {
	// 1. 准备一个大列表，用来装所有中间键值对
	var kva []KeyValue

	// 2. 遍历所有 Map 任务，读取属于当前 Reduce 的中间文件
	for m := 0; m < nMap; m++ {
		// 拼接文件名：mr-第m个Map-第reduceId个Reduce
		iname := fmt.Sprintf("mr-%d-%d", m, reduceId)
		file, err := os.Open(iname)
		if err != nil {
			continue // 文件不存在就跳过
		}

		// 用 JSON 解码器读取文件内容
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// 从文件里解码出一个键值对
			if err := dec.Decode(&kv); err != nil {
				break // 读完了就退出
			}
			// 把键值对加到大列表里
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 3. 按 Key 字典序排序！
	// 这一步非常关键：把相同的 Key 全部排在一起
	// 相同 key 会挨在一起 才能合并key
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// 4. 创建最终输出文件（先临时文件，再改名）
	oname := fmt.Sprintf("mr-out-%d", reduceId)
		tmp, _ := os.CreateTemp(".", "out-*")

	// 5. 遍历排序后的列表，按 Key 分组，调用 reducef
	i := 0
	for i < len(kva) {
		// 找到所有和 i 位置 Key 相同的连续元素，直到 j
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		// 把相同 Key 的所有 Value 收集到一个列表里
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		// 调用你写的 reducef：传入 key + [value1, value2, ...]
		output := reducef(kva[i].Key, values)

		// 写入结果：key 结果
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output)

		// 跳到下一个不同的 Key
		i = j
	}

	// 6. 写完后，重命名为正式文件
	tmp.Close()
	os.Rename(tmp.Name(), oname)
}

// 定义RPC调用函数：
// 参数：rpcname 方法名 / args 入参 / reply 出参
// 返回值：调用成功返回true，失败会直接退出程序
func call(rpcname string, args interface{}, reply interface{}) bool {
	// 1. 通过HTTP协议，连接Unix域套接字的Coordinator
	// coordSockName：提前定义好的套接字文件路径（本地进程间通信）
	c, err := rpc.DialHTTP("unix", coordSockName)
	// 2. 连接失败处理
	if err != nil {
		// 连接失败 = 协调器已经退出/挂了
		// 工作节点直接退出程序（退出码0表示正常退出）
		os.Exit(0)
	}
	// 3. 延迟关闭连接：函数执行完毕后，自动关闭RPC连接
	defer c.Close()
	/*
	   func (client *Client) Call(serviceMethod string, args any, reply any) error {
	   	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	   	return call.Error
	   }

	*/
	// 4. 发起真正的RPC远程调用
	err = c.Call(rpcname, args, reply)
	// 5. 调用失败处理
	if err != nil {
		// 调用失败 = 协调器已退出/失联
		os.Exit(0)
	}
	// 6. 所有步骤成功，返回true
	return true
}
