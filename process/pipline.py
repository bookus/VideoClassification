import os
import asyncio
import sys
import multiprocessing
import time
import json

queue = asyncio.Queue()

bucket = "mmimg"
ak = "8MbTywnGQZ75BnWL9S1P8PZn-9wCqy6fIs4MyllI"
sc = "XXXXXXX"

train_path = "lsvc2017/lsvc_train.txt"

val_path = "lsvc2017/lsvc_val.txt"

train_val_path = "/workspace/data/video/videos/trainval"

test_path = "/workspace/data/video/videos/test"


trainval_map = {}
trainvaltest_set_map = {}

def init():




def split(line):
    return line.split(',', 1)


class Consumer(multiprocessing.Process):
    def __init__(self, task_queue, result_queue,ID):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.ID=ID
        self.temp_path="./temp"+str(ID)
        os.mkdir(self.temp_path)
        self.qupload_config_file = self.write_qupload_config_file(self.temp_path)


    def upload(self,file):
        file_path=self.temp_path+"/"+file

        os.system()

    def write_qupload_config_file(self, upload_dir):
        config_json = dict()
        config_json['src_dir'] = upload_dir
        config_json['bucket'] = bucket
        config_json['key_prefix'] = ''

        qupload_config_file = upload_dir + '.conf'
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

            file_path=os.path.join(train_val_path,file)

            cmd='./export_frames -i {} -interval 4 -c 21 -o {} -s 256x256 -postfix jpg'.format(file_path,self.temp_path)
            print(cmd)
            #执行算光流
            os.system(cmd)

            up_files=os.listdir(self.temp_path)

            for file in up_files:
                #/[test/train/val]/[label]/[filename][frame/flow][序列].jpg
                video_name = file.split('.')[0]
                video_set = trainvaltest_set_map[video_name]
                video_label = trainval_map[video_name]
                new_file = video_set + '/' + video_label + '/' + file
                os.rename(file, new_file)

            cmd = './qshell qupload {} {}'.format(len(up_files), self.qupload_config_file)
            print cmd
            os.system(cmd)


            self.task_queue.task_done()
            #self.result_queue.put(answer)
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
            trainvaltest_set_map[split_list[0]] = 'train'

    with open(val_path, "w") as f:
        for line in f:
            split_list = split(line)
            trainval_map[split_list[0]] = split_list[1]
            trainvaltest_set_map[split_list[0]] = 'val'

    # 读取trainval文件列表

    # 创建消息队列
    tasks = multiprocessing.JoinableQueue()
    results = multiprocessing.Queue()

    # 开始 消费
    num_consumers = multiprocessing.cpu_count() * 2

    consumers = [Consumer(tasks, results,i)
                 for i in range(num_consumers)]
    for c in consumers:
        c.start()

    # 入消息队列

    for file in producer():
        tasks.put(file)

    # Wait for all of the tasks to finish
    tasks.join()



if __name__ == "__main__":
    main()
