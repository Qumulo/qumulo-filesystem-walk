import os
import io
import argparse

from typing import Optional, Sequence

from . import FileInfo, Worker


class ChangeExtension:
    def __init__(self, args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        parser.add_argument("--from", help="", required=True, dest="ext_from")
        parser.add_argument("--to", help="", required=True, dest="ext_to")
        self.ARGS = parser.parse_args(args)

    @staticmethod
    def change_extension(
        file_obj: FileInfo, work_obj: Worker["ChangeExtension"]
    ) -> Optional[str]:
        ext_from = work_obj.run_class.ARGS.ext_from
        ext_to = work_obj.run_class.ARGS.ext_to
        if file_obj["path"][-len(ext_from) :] == ext_from:
            (dir_name, from_file_name) = os.path.split(file_obj["path"])
            to_file_name = from_file_name.replace(ext_from, ext_to)
            if work_obj.MAKE_CHANGES:
                work_obj.rc.fs.rename(
                    name=to_file_name,
                    source=file_obj["path"],
                    dir_id=file_obj["dir_id"],
                )
            return "%s: %s -> %s" % (dir_name, from_file_name, to_file_name)
        return None

    @staticmethod
    def every_batch(
        file_list: Sequence[FileInfo], work_obj: Worker["ChangeExtension"]
    ) -> None:
        results = []
        for file_obj in file_list:
            result = ChangeExtension.change_extension(file_obj, work_obj)
            if result:
                results.append(result)

        if len(results) > 0:
            with work_obj.result_file_lock:
                with io.open(work_obj.LOG_FILE_NAME, "a", encoding="utf8") as f:
                    for d in results:
                        f.write("%s\n" % d)
                work_obj.action_count.value += len(results)

    @staticmethod
    def work_start(work_obj: Worker["ChangeExtension"]) -> None:
        if os.path.exists(work_obj.LOG_FILE_NAME):
            os.remove(work_obj.LOG_FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker["ChangeExtension"]) -> None:
        pass
