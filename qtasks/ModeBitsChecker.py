import argparse
import io
import os

from typing import Sequence

from . import FileInfo, Worker


class ModeBitsChecker:
    FILE_NAME = "mode-bits-log.txt"

    def __init__(self, in_args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        _args = parser.parse_args(in_args)

    def every_batch(  # pylint: disable=no-self-use
        self, file_list: Sequence[FileInfo], work_obj: Worker
    ) -> None:
        action_count = 0
        mb_res = []
        for file_obj in file_list:
            if file_obj["mode"][-1] == "0":
                mb_res.append("%(mode)s - %(path)s" % file_obj)

        with work_obj.result_file_lock:
            with io.open(ModeBitsChecker.FILE_NAME, "a", encoding="utf8") as f:
                for line in mb_res:
                    f.write(line + "\n")
            work_obj.action_count.value += action_count

    @staticmethod
    def work_start(_work_obj: Worker) -> None:
        if os.path.exists(ModeBitsChecker.FILE_NAME):
            os.remove(ModeBitsChecker.FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker) -> None:
        return
