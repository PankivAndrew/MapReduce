import logging as logger
import string
import threading
from tqdm import tqdm


class MapReduce(object):
    def __init__(self, file_path, num_mappers=10, num_reducers=10, num_combiners=10, combine=False):
        logger.basicConfig(filename='logs.log', level=logger.DEBUG)
        self.finish_reading = False
        self.finish_shuffling = False
        self.finish_mapping = False

        self.lines = []
        self.file_path = file_path

        self.map_lists = []
        self.combine_dicts = []
        self.shuffle_dict = {}

        self.num_reducers = num_reducers

        self.result_dict = {}

        if '.csv' in self.file_path:
            self.reader = self.create_reader(self.read_csv_work())
        else:
            self.reader = self.create_reader(self.read_txt_work())

        self.mappers = self.create_workers(num_mappers, self.map_work)

        if combine:
            logger.info("Use combiners")
            self.combiners = self.create_workers(num_combiners, self.combine_work)
            self.shuffle()
        else:
            self.combine_shuffle()

    def combine_shuffle(self):
        self.reader.join()
        self.finish_reading = True
        logger.info("Join reader")
        self.joiner(self.mappers)
        logger.info("Join mappers")
        for lst in tqdm(self.map_lists):
            for key, value in lst:
                if key in self.shuffle_dict:
                    self.shuffle_dict[key].append(value)
                else:
                    self.shuffle_dict[key] = [value]
        self.joiner(self.create_workers(self.num_reducers, self.reduce_work))
        return self.result_dict

    def shuffle(self):
        self.reader.join()
        self.finish_reading = True
        logger.info("Join reader")
        self.joiner(self.mappers)
        self.finish_mapping = True
        logger.info("Join mappers")
        self.joiner(self.combiners)
        logger.info("Join combiners")
        for dic in tqdm(self.combine_dicts):
            for key, value in dic.items():
                if key in self.shuffle_dict:
                    self.shuffle_dict[key].append(value)
                else:
                    self.shuffle_dict[key] = [value]
        self.joiner(self.create_workers(self.num_reducers, self.reduce_work))
        return self.result_dict

    def get_result(self):
        return self.result_dict

    @staticmethod
    def remove_punctuation(line):
        return ''.join(ch if ch not in string.punctuation else ' ' for ch in line)

    @staticmethod
    def map(line):
        return [(word, 1) for word in line.split()]

    @staticmethod
    def combine(lst):
        return_dic = {}
        for key, values in lst:
            if key in return_dic:
                return_dic[key] += values
            else:
                return_dic[key] = values
        return return_dic

    def map_work(self):
        while (not self.finish_reading) or len(self.lines) != 0:
            if len(self.lines) != 0:
                try:
                    self.map_lists.append(self.map(self.lines.pop(0)))
                except IndexError:
                    continue

    def combine_work(self):
        while len(self.map_lists) != 0 or not self.finish_mapping:
            if len(self.map_lists) != 0:
                self.combine_dicts.append(self.combine(self.map_lists.pop(0)))

    def reduce_work(self):
        while len(self.shuffle_dict) != 0:
            if len(self.shuffle_dict) != 0:
                try:
                    key, value = self.shuffle_dict.popitem()
                    self.result_dict[key] = sum(value)
                except KeyError:
                    break

    def read_txt_work(self):
        with open(self.file_path) as file:
            for line in file:
                self.lines.append(self.remove_punctuation(line).lower())

    def read_csv_work(self):
        import csv
        with open(self.file_path) as csv_file:
            csv_file = csv.reader(csv_file)
            for row in csv_file:
                self.lines.append(self.remove_punctuation(' '.join(row)).lower())

    @staticmethod
    def create_workers(num_workers, target):
        threads = []
        for _ in range(num_workers):
            t = threading.Thread(target=target)
            threads.append(t)
            t.start()
        return threads

    @staticmethod
    def create_reader(target):
        t = threading.Thread(target=target)
        t.start()
        return t

    @staticmethod
    def joiner(threads):
        for thread in threads:
            thread.join()
