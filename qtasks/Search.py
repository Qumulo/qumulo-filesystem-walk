import re
import os
import io
import argparse

class Search:
    def __init__(self, args):
        parser = argparse.ArgumentParser(description='')
        parser.add_argument('--re', help='', dest="search_re")
        parser.add_argument('--str', help='', dest="search_str")
        parser.add_argument('--cols', help='')
        args = parser.parse_args(args)
        self.search_str = None
        self.search_re = None
        self.cols = ['path']
        if args.search_re:
            self.search_re = re.compile(args.search_re, re.IGNORECASE)
        if args.search_str:
            self.search_str = args.search_str
        if args.cols:
            self.cols = args.cols.split(',')

    @staticmethod
    def every_batch(file_list, work_obj):
        results = []
        for file_obj in file_list:
            if work_obj.run_class.search_str:
                if work_obj.run_class.search_str in file_obj['path']:
                    if "name" in work_obj.run_class.cols:
                        file_obj["name"] = re.sub(r'[|\r\n\\]+', '', file_obj["name"])
                    line = '|'.join([file_obj[col] for col in work_obj.run_class.cols])
                    results.append(line)
            elif work_obj.run_class.search_re:
                if work_obj.run_class.search_re.match(file_obj['path']):
                    if "name" in work_obj.run_class.cols:
                        file_obj["name"] = re.sub(r'[|\r\n\\]+', '', file_obj["name"])
                    line = '|'.join([file_obj[col] for col in work_obj.run_class.cols])
                    results.append(line)

        if len(results) > 0:
            with work_obj.result_file_lock:
                fw = io.open(work_obj.LOG_FILE_NAME, "a", encoding='utf8')
                for d in results:
                    fw.write("%s\n" % d)
                fw.close()
                work_obj.action_count.value += len(results)

    @staticmethod
    def work_start(work_obj):
        if os.path.exists(work_obj.LOG_FILE_NAME):
            os.remove(work_obj.LOG_FILE_NAME)

    @staticmethod
    def work_done(work_obj):
        pass