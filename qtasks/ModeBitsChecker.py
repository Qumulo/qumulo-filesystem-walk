import os
import io

from typing import Sequence

from . import FileInfo, Worker


class ModeBitsChecker:
    FILE_NAME = "mode-bits-log.txt"

    @staticmethod
    def every_batch(
        file_list: Sequence[FileInfo], work_obj: Worker["ModeBitsChecker"]
    ) -> None:
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
    def work_start(_work_obj: Worker["ModeBitsChecker"]) -> None:
        if os.path.exists(ModeBitsChecker.FILE_NAME):
            os.remove(ModeBitsChecker.FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker["ModeBitsChecker"]) -> None:
        return
