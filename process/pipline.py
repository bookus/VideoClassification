import os
import asyncio
import sys
import multiprocessing
import time

queue = asyncio.Queue()

bucket = "mmimg"
ak = "8MbTywnGQZ75BnWL9S1P8PZn-9wCqy6fIs4MyllI"
sc = "XXXXXXX"

train_path = "lsvc2017/lsvc_train.txt"
val_path = "lsvc2017/lsvc_val.txt"


train_val_path = "/workspace/data/video/videos/trainval"

trainval_map = {}

def split(line):
    return line.split(',', 1)


class Consumer(multiprocessing.Process):
    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # 可以退出

                self.task_queue.task_done()
                break

            answer = next_task()
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return


def producer():
    trainval_files = os.listdir(train_val_path)

    for file in trainval_files:
        yield file



def main():
    # 将label数据读入
    with open(train_path, 'w') as f:
        for line in f:
            split_list = split(line)
            trainval_map[split_list[0]] = split_list[1]

    with open(val_path, "w") as f:
        for line in f:
            split_list = split(line)
            trainval_map[split_list[0]] = split_list[1]

    # 读取trainval文件列表

    # 创建消息队列
    tasks = multiprocessing.JoinableQueue()
    results = multiprocessing.Queue()

    # 开始 消费
    num_consumers = multiprocessing.cpu_count() * 2

    consumers = [Consumer(tasks, results)
                 for i in range(num_consumers)]
    for c in consumers:
        c.start()

    # 入消息队列

    for file in producer():
        tasks.put(file)

    # Wait for all of the tasks to finish
    tasks.join()

    os.system('./export_frames -i lsvc058603.avi -interval 4 -c 21 -o ./pics -s 256x256 -postfix jpg')


if __name__ == "__main__":
    main()
