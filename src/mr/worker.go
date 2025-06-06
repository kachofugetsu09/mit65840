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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey 用于对KeyValue数组按Key排序 直接复制粘贴提供的代码中的内容
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 根据进程生成一个简单的Worker ID
	workerId := os.Getpid()
	log.Printf("Worker %d 启动", workerId)

	// Worker主循环
	for {
		// 1. 向Coordinator请求任务
		task := requestTask(workerId)

		// 2. 根据任务类型处理
		switch task.TaskType {
		case MapTask:
			log.Printf("Worker %d 收到Map任务 %d", workerId, task.TaskId)
			doMapTask(task, mapf)

		case ReduceTask:
			log.Printf("Worker %d 收到Reduce任务 %d", workerId, task.TaskId)
			doReduceTask(task, reducef)

		case WaitingTask:
			log.Printf("Worker %d 没有任务，等待中...", workerId)
			time.Sleep(time.Second) // 等待1秒后重试
			continue

		case ExitingTask:
			log.Printf("Worker %d 收到退出信号", workerId)
			return
		}

		// 3. 完成任务后通知Coordinator
		finishTask(workerId, task.TaskType, task.TaskId)
	}
}

// doReduceTask 执行Reduce任务
func doReduceTask(task *RequestTaskReply, reducef func(string, []string) string) {
	log.Printf("开始执行Reduce任务 %d，需要处理 %d 个Map任务的输出", task.TaskId, task.NMap)

	// 1. 读取所有中间文件 mr-X-Y，其中Y是当前Reduce任务的ID
	intermediate := []KeyValue{}

	for mapIndex := 0; mapIndex < task.NMap; mapIndex++ {
		// 构建中间文件名：mr-mapIndex-reduceIndex
		filename := fmt.Sprintf("mr-%d-%d", mapIndex, task.TaskId)

		file, err := os.Open(filename)
		if err != nil {
			log.Printf("警告：无法打开中间文件 %s: %v", filename, err)
			continue // 继续处理其他文件
		}

		// 解码JSON数据
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break // 读取完毕
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	log.Printf("Reduce任务 %d 共读取到 %d 个键值对", task.TaskId, len(intermediate))

	// 2. 按Key排序
	sort.Sort(ByKey(intermediate))

	// 3. 创建临时输出文件（在当前目录创建）
	tempFile, err := ioutil.TempFile(".", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("Reduce任务 %d 创建临时文件失败: %v", task.TaskId, err)
	}

	// 4. 对相同的Key进行Reduce操作
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 找到所有相同Key的值
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// 收集所有相同Key的Value
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// 调用Reduce函数
		output := reducef(intermediate[i].Key, values)

		// 写入输出文件（格式：key value）
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// 5. 关闭临时文件并同步到磁盘
	err = tempFile.Sync() // 确保数据写入磁盘
	if err != nil {
		log.Fatalf("Reduce任务 %d 同步临时文件失败: %v", task.TaskId, err)
	}
	tempFile.Close()

	// 6. 原子重命名为最终输出文件
	outname := fmt.Sprintf("mr-out-%d", task.TaskId)
	err = os.Rename(tempFile.Name(), outname)
	if err != nil {
		log.Fatalf("Reduce任务 %d 重命名输出文件失败: %v", task.TaskId, err)
	}

	log.Printf("Reduce任务 %d 完成，输出文件: %s", task.TaskId, outname)
}

// doMapTask 执行Map任务
func doMapTask(task *RequestTaskReply, mapf func(string, string) []KeyValue) {
	log.Printf("开始执行Map任务 %d，处理文件: %s", task.TaskId, task.File[0])

	// 1. 打开并读取输入文件
	filename := task.File[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Worker打开文件 %s 失败: %v", filename, err)
	}
	defer file.Close()

	// 2. 读取文件内容
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Worker读取文件 %s 失败: %v", filename, err)
	}

	// 3. 获取键值对
	log.Printf("调用Map函数处理文件 %s", filename)
	intermediate := mapf(filename, string(content))
	log.Printf("Map函数返回 %d 个键值对", len(intermediate))

	// 4. 创建nReduce个临时文件，用于存储中间结果
	nReduce := task.NReduce
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)

	// 5. 为每个Reduce任务创建临时文件（在当前目录创建）
	for outindex := 0; outindex < nReduce; outindex++ {
		// 在当前目录创建临时文件，确保原子性
		outFiles[outindex], err = ioutil.TempFile(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("Worker创建临时文件失败: %v", err)
		}
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}

	// 6. 将键值对根据hash分配到不同的文件中
	for _, kv := range intermediate {
		// 计算这个key应该分配给哪个reduce任务
		outindex := ihash(kv.Key) % nReduce

		// 将键值对编码为JSON并写入对应的临时文件
		err := fileEncs[outindex].Encode(&kv)
		if err != nil {
			log.Fatalf("Worker编码KeyValue失败: %v", err)
		}
	}

	// 7. 关闭临时文件并原子重命名为最终的中间文件
	for outindex, file := range outFiles {
		file.Close()

		// 构建最终文件名: mr-X-Y (X是Map任务ID，Y是Reduce任务ID)
		outname := fmt.Sprintf("mr-%d-%d", task.TaskId, outindex)

		// 原子重命名操作，确保文件完整性
		err := os.Rename(file.Name(), outname)
		if err != nil {
			log.Fatalf("Worker重命名文件失败: %v", err)
		}
	}

	log.Printf("Map任务 %d 完成，生成了 %d 个中间文件", task.TaskId, nReduce)
}

// requestTask 向Coordinator请求任务
func requestTask(workerId int) *RequestTaskReply {
	// 1. 准备RPC参数
	args := RequestTaskArgs{
		WorkerId: workerId,
	}

	// 2. 准备接收回复的结构
	reply := RequestTaskReply{}

	// 3. 发起RPC调用
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		log.Printf("Worker %d 请求任务失败", workerId)
		// 如果RPC失败，说明Coordinator已经退出，Worker应该退出
		reply.TaskType = ExitingTask
	}

	return &reply
}

// finishTask 通知Coordinator任务完成
func finishTask(workerId int, taskType TaskType, taskId int) {
	// 1. 准备RPC参数
	args := FinishTaskArgs{
		WorkerId: workerId,
		TaskType: taskType,
		TaskId:   taskId,
	}

	// 2. 准备接收回复的结构
	reply := FinishTaskReply{}

	// 3. 发起RPC调用
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		log.Printf("Worker %d 通知任务完成失败", workerId)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing:%v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
