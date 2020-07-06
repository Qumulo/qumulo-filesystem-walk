import re
import os
import io
import zlib
import math
import argparse
import random

class Search:
    ARGS = None
    def __init__(self, args):
        parser = argparse.ArgumentParser(description='')
        parser.add_argument('--re', help='', dest="search_re")
        parser.add_argument('--str', help='', dest="search_str")
        args = parser.parse_args(args)
        self.search_str = None
        self.search_re = None
        if args.search_re:
            self.search_re = re.compile(args.search_re, re.IGNORECASE)
        if args.search_str:
            self.search_str = args.search_str

    @staticmethod
    def every_batch(file_list, work_obj):
        results = []
        for file_obj in file_list:
            if work_obj.run_class.search_str:
                if work_obj.run_class.search_str in file_obj['path']:
                    results.append(file_obj['path'])
            elif work_obj.run_class.search_re:
                if work_obj.run_class.search_re.match(file_obj['path']):
                    results.append(file_obj['path'])

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


class ChangeExtension:
    ARGS = None
    def __init__(self, args):
        parser = argparse.ArgumentParser(description='')
        parser.add_argument('--from', help='', required=True, dest="ext_from")
        parser.add_argument('--to', help='', required=True, dest="ext_to")
        self.ARGS = parser.parse_args(args)

    @staticmethod
    def change_extension(file_obj, work_obj):
        ext_from = work_obj.run_class.ARGS.ext_from
        ext_to   = work_obj.run_class.ARGS.ext_to
        if file_obj['path'][-len(ext_from):] == ext_from:
            (dir_name, from_file_name) = os.path.split(file_obj['path'])
            to_file_name = from_file_name.replace(ext_from, ext_to)
            if work_obj.MAKE_CHANGES:
                work_obj.rc.fs.rename(name = to_file_name,
                                      source = file_obj['path'],
                                      dir_id = file_obj["dir_id"],
                                      )
            return "%s: %s -> %s" % (dir_name, from_file_name, to_file_name)

    @staticmethod
    def every_batch(file_list, work_obj):
        results = []
        for file_obj in file_list:
            result = ChangeExtension.change_extension(file_obj, work_obj)
            if result:
                results.append(result)

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


class SummarizeOwners:
    FILE_NAME = "owners.txt"

    @staticmethod
    def every_batch(file_list, work_obj):
        owners = {}
        for file_obj in file_list:
            k = file_obj['owner'] + "|%(id_type)s|%(id_value)s" % file_obj['owner_details']
            if k not in owners:
                owners[k] = {"count": 1, "size": int(file_obj['size'])}
            else:
                owners[k]["count"] += 1
                owners[k]["size"] += int(file_obj['size'])
        with work_obj.result_file_lock:
            fw = io.open(SummarizeOwners.FILE_NAME, "a", encoding='utf8')
            for k, v in owners.items():
                fw.write("%s|%s|%s\n" % (k, v["count"], v["size"]))
            fw.close()
            work_obj.action_count.value += 1
        return None

    @staticmethod
    def work_done(work_obj):
        print("-"*80)
        fr = io.open(SummarizeOwners.FILE_NAME, "r", encoding='utf8')
        owners = {}
        for line in fr:
            (owner, id_type, id_value, count, size) = line.split('|')
            count = int(count)
            size = int(size)
            if owner not in owners:
                owners[owner] = {"owner":owner, "id_type":id_type, "id_value":id_value, "count":count, "size":size}
            else:
                owners[owner]["count"] += count
                owners[owner]["size"] += size
        fr.close()
        for k, v in owners.items():
            print("%(owner)12s (%(id_type)10s/%(id_value)48s): %(count)9s / %(size)15s" % v)
        os.remove(SummarizeOwners.FILE_NAME)
        print("-"*80)

    @staticmethod
    def work_start(work_obj):
        if os.path.exists(SummarizeOwners.FILE_NAME):
            os.remove(SummarizeOwners.FILE_NAME)


class DataReductionTest:
    FILE_NAME = "data-reduction-test-results.txt"

    def __init__(self, args = None):
        parser = argparse.ArgumentParser(description='')
        parser.add_argument('--perc', help='', dest="perc")
        args = parser.parse_args(args)
        self.sample_perc = 0.05
        if args.perc:
            self.sample_perc = float(args.perc)

    @staticmethod
    def compress_it(work_obj, file_id, offset):
        fw = io.BytesIO()
        work_obj.rc.fs.read_file(file_ = fw, 
                                 id_ = file_id, 
                                 offset = offset, 
                                 length = 4096)
        fw.seek(0)
        c_len = len(zlib.compress(fw.read(), 4))
        c_level = int(round(10 * c_len / 4096.0, 0))
        if c_level == 10:
            c_level = 9
        return c_level

    @staticmethod
    def every_batch(file_list, work_obj):
        res = []
        action_count = 0
        for file_obj in file_list:
            if file_obj["type"] == 'FS_FILE_TYPE_FILE':
                # sample 5% of files
                if random.random() < (1-work_obj.run_class.sample_perc):
                    continue
                action_count += 1
                file_size = int(file_obj['size'])
                try:
                    c_start = DataReductionTest.compress_it(work_obj, file_obj["id"], 0)
                except:
                    continue
                c_end = 'x'
                c_middle = 'x'
                if file_size > 4096*2:
                    try:
                        c_end = DataReductionTest.compress_it(work_obj, file_obj["id"]
                                                                , file_size-4096)
                    except:
                        continue
                if file_size > 4096*3:
                    try:
                        c_middle = DataReductionTest.compress_it(work_obj, file_obj["id"]
                                        , math.floor((file_size/2.0)/4096)*4096)
                    except:
                        continue
                ext = file_obj['name'].rpartition('.')[-1]
                ext = ext.encode('ascii', 'ignore')
                if len(ext) > 6:
                    ext = ext[0:6]
                res.append("%s%s%s|%s|%s" % (c_start, c_middle, c_end, ext, file_obj["size"]))
                if action_count > 100:
                    with work_obj.result_file_lock:
                        work_obj.action_count.value += action_count
                    action_count = 0

        with work_obj.result_file_lock:
            fw = io.open(DataReductionTest.FILE_NAME, "a+", encoding='utf8')
            for line in res:
                fw.write(line + "\n")
            fw.close()
            work_obj.action_count.value += action_count
        return None

    @staticmethod
    def work_start(work_obj):
        if os.path.exists(DataReductionTest.FILE_NAME):
            os.remove(DataReductionTest.FILE_NAME)

    @staticmethod
    def work_done(work_obj):
        return


class ModeBitsChecker:
    FILE_NAME = "mode-bits-log.txt"

    @staticmethod
    def every_batch(file_list, work_obj):
        res = []
        action_count = 0
        mb_res = []
        for file_obj in file_list:
            if file_obj["mode"][1] == '0':
                mb_res.append("%(mode)s - %(path)s" % file_obj)
        with work_obj.result_file_lock:
            fw = io.open(ModeBitsChecker.FILE_NAME, "a", encoding='utf8')
            for line in mb_res:
                fw.write(line + "\n")
            fw.close()
            work_obj.action_count.value += action_count
        return None

    @staticmethod
    def work_start(work_obj):
        if os.path.exists(ModeBitsChecker.FILE_NAME):
            os.remove(ModeBitsChecker.FILE_NAME)

    @staticmethod
    def work_done(work_obj):
        return

