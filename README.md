# -Agent-
3种Agent——采集Agent、处理Agent、存储Agent，通过队列协同，自动流转任务
import queue
import threading
import time
import random
import enum

# ================= 任务定义 =================
class TaskType(enum.Enum):
    DATA_COLLECT = 1
    DATA_PROCESS = 2
    DATA_STORE   = 3

class Task:
    def __init__(self, task_id, task_type, payload=None):
        self.id = task_id
        self.type = task_type
        self.payload = payload
        self.result = None
        self.status = "created"

    def __repr__(self):
        return f"Task({self.id}, {self.type.name}, status={self.status})"

# ================= Agent基类 =================
class Agent(threading.Thread):
    def __init__(self, name, input_queue, output_queue):
        super().__init__()
        self.name = name
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.running = True

    def run(self):
        while self.running:
            try:
                task = self.input_queue.get(timeout=1)
                if task is None:  # 毒丸，停止信号
                    break
                self.process(task)
                if self.output_queue:
                    self.output_queue.put(task)
            except queue.Empty:
                continue
        print(f"{self.name} 退出")

    def process(self, task):
        raise NotImplementedError

    def stop(self):
        self.running = False

# ================= 具体Agent实现 =================
class CollectAgent(Agent):
    def process(self, task):
        if task.type != TaskType.DATA_COLLECT:
            return
        print(f"[{self.name}] 采集数据, task {task.id}")
        time.sleep(random.uniform(0.1, 0.5))  # 模拟采集耗时
        task.payload = f"数据_{task.id}"
        task.type = TaskType.DATA_PROCESS  # 转为处理类型
        task.status = "collected"

class ProcessAgent(Agent):
    def process(self, task):
        if task.type != TaskType.DATA_PROCESS:
            return
        print(f"[{self.name}] 处理数据: {task.payload}, task {task.id}")
        time.sleep(random.uniform(0.2, 0.8))
        task.payload = task.payload.upper() + "_已处理"
        task.type = TaskType.DATA_STORE
        task.status = "processed"

class StoreAgent(Agent):
    def process(self, task):
        if task.type != TaskType.DATA_STORE:
            return
        print(f"[{self.name}] 存储结果: {task.payload}, task {task.id}")
        time.sleep(random.uniform(0.1, 0.3))
        task.status = "stored"
        # 输出队列为空，任务结束

# ================= 协同调度器 =================
class Coordinator:
    def __init__(self):
        # 三级流水线队列
        self.collect_queue = queue.Queue()
        self.process_queue = queue.Queue()
        self.store_queue   = queue.Queue()

        # 创建Agents
        self.collector = CollectAgent("Collector-1", self.collect_queue, self.process_queue)
        self.processor1 = ProcessAgent("Processor-1", self.process_queue, self.store_queue)
        self.processor2 = ProcessAgent("Processor-2", self.process_queue, self.store_queue)
        self.storer = StoreAgent("Storer-1", self.store_queue, None)  # 最后一级无输出

    def start(self):
        self.collector.start()
        self.processor1.start()
        self.processor2.start()
        self.storer.start()
        print("=== 多Agent系统启动 ===")

    def submit_tasks(self, num):
        """提交采集任务"""
        for i in range(num):
            task = Task(i, TaskType.DATA_COLLECT)
            self.collect_queue.put(task)
            print(f"提交任务 {i}")

    def shutdown(self):
        """优雅停止: 发送毒丸"""
        # 等待采集队列空
        self.collect_queue.join()
        self.collect_queue.put(None)  # 通知采集Agent退出

        # 等待处理队列空
        self.process_queue.join()
        self.process_queue.put(None)
        self.process_queue.put(None)  # 给两个处理器各一个毒丸

        # 等待存储队列空
        self.store_queue.join()
        self.store_queue.put(None)

        # 等待所有线程结束
        self.collector.join()
        self.processor1.join()
        self.processor2.join()
        self.storer.join()
        print("=== 系统关闭 ===")

# ================= 主程序 =================
if __name__ == "__main__":
    coordinator = Coordinator()
    coordinator.start()
    coordinator.submit_tasks(10)   # 投10个任务
    time.sleep(5)                 # 让系统运行一会儿
    coordinator.shutdown()
