import re
import os
import io
import zlib
import math


class ChangeExtension:

    @staticmethod
    def change_extension(file_obj, work_obj):
        ext_from = ".jpeg"
        ext_to   = ".jpg"
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
                fw = open(work_obj.LOG_FILE_NAME, "a")
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
            fw = open(SummarizeOwners.FILE_NAME, "a")
            for k, v in owners.items():
                fw.write("%s|%s|%s\n" % (k, v["count"], v["size"]))
            fw.close()
            work_obj.action_count.value += 1
        return None

    @staticmethod
    def work_done(work_obj):
        print("")
        print("-"*80)
        fr = open(SummarizeOwners.FILE_NAME, "r")
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
                action_count += 1
                file_size = int(file_obj['size'])
                c_start = DataReductionTest.compress_it(work_obj, file_obj["id"], 0)
                c_end = 'x'
                c_middle = 'x'
                if file_size > 4096*2:
                    c_end = DataReductionTest.compress_it(work_obj, file_obj["id"]
                                                            , file_size-4096)
                if file_size > 4096*3:
                    c_middle = DataReductionTest.compress_it(work_obj, file_obj["id"]
                                        , math.floor((file_size/2.0)/4096)*4096)
                res.append("%s%s%s|%s" % (c_start, c_middle, c_end
                                     , file_obj['name'].rpartition('.')[-1]))
                if action_count > 100:
                    with work_obj.result_file_lock:
                        work_obj.action_count.value += action_count
                    action_count = 0


        with work_obj.result_file_lock:
            fw = open(DataReductionTest.FILE_NAME, "a")
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
            fw = open(ModeBitsChecker.FILE_NAME, "a")
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

