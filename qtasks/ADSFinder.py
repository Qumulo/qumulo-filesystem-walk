import argparse
import io
import os

from typing import Optional, Sequence

from . import FileInfo, Worker


class ADSFinder:
    DEFAULT_FILE_NAME = "ads-finder-log"
    DEFAULT_FILE_EXTENSION = ".txt"

    def __init__(self, in_args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        _args = parser.parse_args(in_args)
        # print(_args)

    @staticmethod
    def get_named_streams(file_obj: FileInfo, work_obj: Worker) -> Optional[str]:
        # use work_obj.rc.fs.list_named_streams()
        streams = work_obj.rc.fs.list_named_streams(path=file_obj['path'])
        result = [(f['name'], f['size']) for f in streams]
        return result

    @staticmethod
    def get_logfile_name(path):
        return ADSFinder.DEFAULT_FILE_NAME + \
               path.replace('/', '-') + \
               ADSFinder.DEFAULT_FILE_EXTENSION

    def every_batch(  # pylint: disable=no-self-use
        self, file_list: Sequence[FileInfo], work_obj: Worker
    ) -> None:
        action_count = 0
        mb_res = []
        for file_obj in file_list:
            res = self.get_named_streams(file_obj, work_obj)
            if res:
                for r in res:
                    mb_res.append('%s: %s' % (file_obj['path'], r))

        FILE_NAME = self.get_logfile_name(work_obj.start_path)
        with work_obj.result_file_lock:
            with io.open(FILE_NAME, "a", encoding="utf8") as f:
                for line in mb_res:
                    f.write(line + "\n")
            work_obj.action_count.value += action_count

    @staticmethod
    def work_start(_work_obj: Worker) -> None:
        FILE_NAME = ADSFinder.get_logfile_name(_work_obj.start_path)
        if os.path.exists(FILE_NAME):
            os.remove(FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker) -> None:
        return
