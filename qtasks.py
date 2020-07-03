import re
import os


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
