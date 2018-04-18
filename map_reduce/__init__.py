"""
MapReduce multi-threading algorithm for counting words
"""
import logging as logger
import string
import threading
from tqdm import tqdm


class MapReduce(object):
    """
    MapReduce class for counting words
    """
    def __init__(self, file_path, num_mappers=10, num_reducers=10, num_combiners=10, combine=False):
        """
        MapReduce class constructor

        :param file_path: path to file in which unique words
        :param num_mappers: number of mappers
        :param num_reducers: number of reducers
        :param num_combiners: number of combiners
        :param combine: bool to use combiners or not
        """
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
        """
        Combine and shuffle from all maps

        :return: combined and shuffled dictionary
        """
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
        """
        Shuffle from all maps

        :return: shuffled dictionary
        """
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
        """
        :return: return result dictionary
        """
        return self.result_dict

    @staticmethod
    def remove_punctuation(line):
        """
        Change all punctuation chars with space

        :param line: line to be cleaned from punctuation
        :return:cleaned line
        """
        return ''.join(ch if ch not in string.punctuation else ' ' for ch in line)

    @staticmethod
    def map(line):
        """
        Simple map function. Create python list of tuples (<word>, 1)

        :param line: line from where we store words to list
        :return: list of tuples
        """
        return [(word, 1) for word in line.split()]

    @staticmethod
    def combine(lst):
        """
        Combine python list of tuples to dictionary

        :param lst: python list of tuples
        :return: combined dictionary
        """
        return_dic = {}
        for key, values in lst:
            if key in return_dic:
                return_dic[key] += values
            else:
                return_dic[key] = values
        return return_dic

    def map_work(self):
        """
        Mapper thread method
        """
        while (not self.finish_reading) or len(self.lines) != 0:
            if len(self.lines) != 0:
                try:
                    self.map_lists.append(self.map(self.lines.pop(0)))
                except IndexError:
                    continue

    def combine_work(self):
        """
        Combine thread method
        """
        while len(self.map_lists) != 0 or not self.finish_mapping:
            if len(self.map_lists) != 0:
                self.combine_dicts.append(self.combine(self.map_lists.pop(0)))

    def reduce_work(self):
        """
        Reduce thread method
        """
        while len(self.shuffle_dict) != 0:
            if len(self.shuffle_dict) != 0:
                try:
                    key, value = self.shuffle_dict.popitem()
                    self.result_dict[key] = sum(value)
                except KeyError:
                    break

    def read_txt_work(self):
        """
        Txt reader thread method
        """
        with open(self.file_path) as file:
            for line in file:
                self.lines.append(self.remove_punctuation(line).lower())

    def read_csv_work(self):
        """
        Csv reader thread method
        """
        import csv
        with open(self.file_path) as csv_file:
            csv_file = csv.reader(csv_file)
            for row in csv_file:
                self.lines.append(self.remove_punctuation(' '.join(row)).lower())

    @staticmethod
    def create_workers(num_workers, target):
        """
        Template function for workers(threads) creation

        :param num_workers: number of workers(threads)
        :param target: target method for worker(thread)
        :return: list of workers(threads)
        """
        threads = []
        for _ in range(num_workers):
            t = threading.Thread(target=target)
            threads.append(t)
            t.start()
        return threads

    @staticmethod
    def create_reader(target):
        """
        Template function for reader(thread) creation

        :param target: target method for reader(thread)
        :return: reader(thread)
        """
        t = threading.Thread(target=target)
        t.start()
        return t

    @staticmethod
    def joiner(threads):
        """
        Loop through all threads to join() them

        :param threads: threads to join
        """
        for thread in threads:
            thread.join()
