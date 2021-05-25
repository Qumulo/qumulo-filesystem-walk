import argparse
import io
import os

from typing import Optional, Sequence

from . import FileInfo, Worker


class ChangeExtension:
    def __init__(self, args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        parser.add_argument("--from", help="", required=True, dest="ext_from")
        parser.add_argument("--to", help="", required=True, dest="ext_to")
        self.ARGS = parser.parse_args(args)

    def change_extension(self, file_obj: FileInfo, work_obj: Worker) -> Optional[str]:
        ext_from = self.ARGS.ext_from
        ext_to = self.ARGS.ext_to
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

    def every_batch(self, file_list: Sequence[FileInfo], work_obj: Worker) -> None:
        results = []
        for file_obj in file_list:
            result = self.change_extension(file_obj, work_obj)
            if result:
                results.append(result)

        if len(results) > 0:
            with work_obj.result_file_lock:
                with io.open(work_obj.LOG_FILE_NAME, "a", encoding="utf8") as f:
                    for d in results:
                        f.write("%s\n" % d)
                work_obj.action_count.value += len(results)

    @staticmethod
    def work_start(work_obj: Worker) -> None:
        if os.path.exists(work_obj.LOG_FILE_NAME):
            os.remove(work_obj.LOG_FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker) -> None:
        pass
