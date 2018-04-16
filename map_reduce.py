import string
import threading
import time
from tqdm import tqdm


class MapReduce(object):
    def __init__(self, file_path, num_mapers=10, num_combiners=10, num_reducers=10):
        self.start = time.time()

        self.lines = []
        self.file_path = file_path

        self.map_dicts = []
        self.combine_dicts = []
        self.shuffle_lst = []

        self.num_reducers = num_reducers

        self.result_dict = {}

        self.reader = self.create_reader()
        self.mapers = self.create_workers(num_mapers, self.map_work)
        self.combiners = self.create_workers(num_combiners, self.combine_work)

    def shuffle(self):
        self.joiner(self.reader)
        print("Join reader")
        self.joiner(self.mapers)
        print("Join mapers")
        self.joiner(self.combiners)
        print("Join combiners")
        for dic in tqdm(self.combine_dicts):
            for key, value in dic.items():
                matches = [any(key in x for x in lst) for lst in self.shuffle_lst]
                if True in matches:
                    self.shuffle_lst[matches.index(True)].append((key, value))
                else:
                    self.shuffle_lst.append([(key, value)])
        self.joiner(self.create_workers(self.num_reducers, self.reduce_work))
        print(self.result_dict)
        finish = time.time()
        print(finish - self.start)

    def read_work(self):
        with open(self.file_path) as file:
            for line in file:
                self.lines.append(self.remove_punctuation(line).lower())

    @staticmethod
    def remove_punctuation(line):
        return ''.join(ch for ch in line if ch not in string.punctuation).lower()

    @staticmethod
    def map(line):
        return {word: 1 for word in line.split()}

    @staticmethod
    def combine(dic):
        return_dic = {}
        for key, values in dic.items():
            if key in return_dic:
                return_dic[key] += values
            else:
                return_dic[key] = values
        return return_dic

    def map_work(self):
        while len(self.lines) != 0:
            self.map_dicts.append(self.map(self.lines.pop(0)))

    def combine_work(self):
        while len(self.map_dicts) != 0 or len(self.lines) != 0:
            if len(self.map_dicts) != 0:
                self.combine_dicts.append(self.combine(self.map_dicts.pop(0)))

    def reduce_work(self):
        while len(self.shuffle_lst) != 0:
            for index, word in enumerate(self.shuffle_lst.pop(0)):
                if index == 0:
                    self.result_dict[word[0]] = word[1]
                else:
                    self.result_dict[word[0]] += word[1]

    @staticmethod
    def create_workers(num_workers, target):
        threads = []
        for _ in range(num_workers):
            t = threading.Thread(target=target)
            threads.append(t)
            t.start()
        return threads

    def create_reader(self):
        t = threading.Thread(target=self.read_work)
        t.start()
        return [t]

    @staticmethod
    def joiner(threads):
        for thread in threads:
            thread.join()


MapReduce('text1.txt', 10, 10, 10).shuffle()
