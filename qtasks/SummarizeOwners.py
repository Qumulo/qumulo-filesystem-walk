import re
import os
import io

class SummarizeOwners:
    # A temporary file for storing the intermediate walk work
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