import os
import sys
import multiprocessing
import time
import json
import shutil

RESUME = False

TRAIN = True

bucket = "mmflow"
ak = "8MbTywnGQZ75BnWL9S1P8PZn-9wCqy6fIs4MyllI"
sk = "XXXXXXX"

train_path = "lsvc2017/lsvc_train.txt"

val_path = "lsvc2017/lsvc_val.txt"

test_path = "lsvc2017/lsvc_test.txt"

train_val_path = "/workspace/data/video/videos/trainval"

test_file_path = "/workspace/data/video/videos/test"

qupload_dir = "/workspace/mmflow-data/upload"
temp_dir = "/workspace/mmflow-data/temp"

qupload_config_dir = "./config/"

log_file = "log.txt"

trainval_map = {}
trainvaltest_set_map = {}


def init():
    # os.system('./qshell account {} {}'.format(ak, sk))
    if not os.path.exists(qupload_config_dir):
        os.mkdir(qupload_config_dir)
    if not os.path.exists(qupload_dir):
        os.mkdir(qupload_dir)


def split(line):
    return line.split(',', 1)


class Consumer(multiprocessing.Process):
    def __init__(self, task_queue, result_queue, ID):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.ID = ID
        self.temp_path = temp_dir
        if not os.path.exists(self.temp_path):
            os.mkdir(self.temp_path)

    def run(self):
        proc_name = self.name
        while True:
            file = self.task_queue.get()
            if file is None:
                # 可以退出
                self.task_queue.task_done()
                break

            video_name = file.split('.')[0]

            file_path = os.path.join(train_val_path, file)
            dest_dir = os.path.join(self.temp_path, video_name)
            os.mkdir(dest_dir)

            cmd = './export_frames -i {} -interval 10 -c 21 -o {} -s 256x256 -postfix jpg'.format(file_path,
                                                                                                  dest_dir)
            # 执行算光流
            os.system(cmd)

            # if up_files:
            #    cmd = './qshell qupload {} {}'.format(len(up_files), self.qupload_config_file)
            #    os.system(cmd)

            self.task_queue.task_done()
            self.result_queue.put(file)
        return


class Collector(multiprocessing.Process):
    def __init__(self, result_queue):
        super(Collector, self).__init__()
        self.result_queue = result_queue

    def run(self):
        with open(log_file, 'a+') as f:
            while True:
                file = self.result_queue.get()
                if file is None:
                    break

                f.write(file + "\n")

        return


def producer():
    files = []
    if TRAIN:
        files = os.listdir(train_val_path)
    else:
        files = os.listdir(test_file_path)
    # test_files = os.listdir(test_file_path)

    # trainval_files.extend(test_files)
    processed_files = []

    if RESUME:
        with open(log_file, 'r') as f:
            processed_files = [line.strip('\n') for line in f]

    for file in files:
        if RESUME and file in processed_files:
            continue
        yield file


def main():
    # 将label数据读入
    init()

    with open(train_path, 'r') as f:
        for line in f:
            split_list = split(line.strip('\n'))
            trainval_map[split_list[0]] = split_list[1]
            trainvaltest_set_map[split_list[0]] = 'train'

    with open(val_path, "r") as f:
        for line in f:
            split_list = split(line.strip('\n'))
            trainval_map[split_list[0]] = split_list[1]
            trainvaltest_set_map[split_list[0]] = 'val'

    with open(test_path, "r") as f:
        for line in f:
            split_list = split(line.strip('\n'))
            trainval_map[split_list[0]] = "0"
            trainvaltest_set_map[split_list[0]] = 'test'

    # 读取trainval文件列表

    # 创建消息队列
    tasks = multiprocessing.JoinableQueue()
    results = multiprocessing.Queue()

    # 开始 消费
    num_consumers = multiprocessing.cpu_count()
    # num_consumers=100
    consumers = [Consumer(tasks, results, i)
                 for i in range(num_consumers)]
    for c in consumers:
        c.start()

    # 入消息队列

    # 消息收集
    collector = Collector(results)
    collector.start()

    for file in producer():
        tasks.put(file)

    # Wait for all of the tasks to finish
    tasks.join()


if __name__ == "__main__":
    main()
