import os
import io


class ModeBitsChecker:
    FILE_NAME = "mode-bits-log.txt"

    @staticmethod
    def every_batch(file_list, work_obj):
        action_count = 0
        mb_res = []
        for file_obj in file_list:
            if file_obj["mode"][-1] == "0":
                mb_res.append("%(mode)s - %(path)s" % file_obj)

        with work_obj.result_file_lock:
            fw = io.open(ModeBitsChecker.FILE_NAME, "a", encoding="utf8")
            for line in mb_res:
                fw.write(line + "\n")
            fw.close()
            work_obj.action_count.value += action_count

    @staticmethod
    def work_start(work_obj):
        if os.path.exists(ModeBitsChecker.FILE_NAME):
            os.remove(ModeBitsChecker.FILE_NAME)

    @staticmethod
    def work_done(work_obj):
        return
