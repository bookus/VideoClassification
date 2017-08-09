import os
import sys
import multiprocessing
import time
import json
import shutil

RESUME = False

bucket = "mmimg"
ak = "8MbTywnGQZ75BnWL9S1P8PZn-9wCqy6fIs4MyllI"
sk = "XXXXXXX"

train_path = "lsvc2017/lsvc_train.txt"

val_path = "lsvc2017/lsvc_val.txt"

test_path = "lsvc2017/lsvc_test.txt"

train_val_path = "/workspace/data/video/videos/trainval"

test_file_path = "/workspace/data/video/videos/test"

qupload_dir = "/workspace/data/video/videos"

log_file = "log.txt"

trainval_map = {}
trainvaltest_set_map = {}

qupload_config_dir = "./config/"
BASE_DIR = os.path.dirname(os.path.dirname(__file__))


def init():
    os.system('./qshell account {} {}'.format(ak, sk))
    if not os.path.exists(qupload_config_dir):
        os.mkdir(qupload_config_dir)


def split(line):
    return line.split(',', 1)


class Consumer(multiprocessing.Process):
    def __init__(self, task_queue, result_queue, ID):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.ID = ID

        self.temp_path = os.path.join(qupload_dir, "temp" + str(ID))
        os.mkdir(self.temp_path)
        self.qupload_config_file = self.write_qupload_config_file(self.temp_path)

    def upload(self, file, key):
        file_path = self.temp_path + "/" + file
        cmd = './qrsctl put -c {}  '
        os.system()

    def clean(self):
        shutil.rmtree(self.temp_path)
        os.mkdir(self.temp_path)

    def write_qupload_config_file(self, upload_dir):
        config_json = dict()
        config_json['src_dir'] = upload_dir
        config_json['bucket'] = bucket

        qupload_config_file = os.path.join(qupload_config_dir, upload_dir + '.conf')
        with open(qupload_config_file, 'w') as f_out:
            f_out.write(json.dumps(config_json, indent=4) + '\n')
        return qupload_config_file

    def run(self):
        proc_name = self.name
        while True:
            file = self.task_queue.get()
            if file is None:
                # 可以退出

                self.task_queue.task_done()
                break

            video_name = file.split('.')[0]
            video_set = trainvaltest_set_map[video_name]
            video_label = trainval_map[video_name]

            file_path = os.path.join(train_val_path, file)

            if video_label == "-1":
                file_path = os.path.join(test_file_path, file)

            cmd = './export_frames -i {} -interval 10 -c 21 -o {} -s 256x256 -postfix jpg'.format(file_path,
                                                                                                  self.temp_path)
            # 执行算光流
            os.system(cmd)

            up_files = os.listdir(self.temp_path)

            for file in up_files:
                # /[test/train/val]/[label]/[filename][frame/flow][序列].jpg

                new_file = video_set + '-' + video_label + '-' + file

                os.system("mv {} {}".format(os.path.join(self.temp_path, file), os.path.join(self.temp_path, new_file)))

            if len(up_files) > 0:
                cmd = './qshell qupload {} {}'.format(len(up_files), self.qupload_config_file)
                os.system(cmd)

            self.clean()

            self.task_queue.task_done()
            self.result_queue.put(file)
        return


class Collector(multiprocessing.Process):
    def __init__(self, result_queue):
        super(Collector, self).__init__()
        self.result_queue = result_queue

    def run(self):
        with open(log_file, 'w') as f:
            while True:
                file = self.result_queue.get()
                if file is None:
                    break

                f.write(file + "\n")

                self.result_queue.task_done()

        return


def producer():
    trainval_files = os.listdir(train_val_path)
    test_files = os.listdir(test_file_path)

    trainval_files.extend(test_files)

    for file in trainval_files:
        yield file


def main():
    # 将label数据读入
    init()

    if RESUME:
        with open(log_file, 'r') as f:
            processed_files = [line.strip('\n').split('.')[0] for line in f]

    with open(train_path, 'r') as f:
        for line in f:
            split_list = split(line.strip('\n'))
            if RESUME and split_list[0] in processed_files:
                continue
            trainval_map[split_list[0]] = split_list[1]
            trainvaltest_set_map[split_list[0]] = 'train'

    with open(val_path, "r") as f:
        for line in f:
            split_list = split(line.strip('\n'))
            if RESUME and split_list[0] in processed_files:
                continue
            trainval_map[split_list[0]] = split_list[1]
            trainvaltest_set_map[split_list[0]] = 'val'

    with open(test_path, "r") as f:
        for line in f:
            split_list = split(line.strip('\n'))
            if RESUME and split_list[0] in processed_files:
                continue
            trainval_map[split_list[0]] = "-1"
            trainvaltest_set_map[split_list[0]] = 'test'

    # 读取trainval文件列表

    # 创建消息队列
    tasks = multiprocessing.JoinableQueue()
    results = multiprocessing.Queue()

    # 开始 消费
    num_consumers = 100  # multiprocessing.cpu_count() * 2
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
