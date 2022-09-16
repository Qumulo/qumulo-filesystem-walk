import argparse
import io
import os

from typing import Optional, Sequence

import qumulo.commands.fs as fs
import qumulo.rest.fs as rest_fs

from . import FileInfo, Worker


class ADSFinder:
    FILE_NAME = "ads-finder-log.txt"

    def __init__(self, in_args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        _args = parser.parse_args(in_args)

    def get_named_streams(self, file_obj: FileInfo, work_obj: Worker) -> Optional[str]:
        # use work_obj.rc.fs.list_named_streams()
        streams = work_obj.rc.fs.list_named_streams(path=file_obj['path'])
        res = [(f['name'], f['size']) for f in streams]
        result = ' '.join(['%s %s' % (r[0], r[1]) for r in res])
        return result

    def every_batch(  # pylint: disable=no-self-use
        self, file_list: Sequence[FileInfo], work_obj: Worker
    ) -> None:
        action_count = 0
        mb_res = []
        for file_obj in file_list:
            res = self.get_named_streams(file_obj, work_obj)
            if res:
                mb_res.append('%s: %s' % (file_obj['path'], res))

        with work_obj.result_file_lock:
            with io.open(ADSFinder.FILE_NAME, "a", encoding="utf8") as f:
                for line in mb_res:
                    f.write(line + "\n")
            work_obj.action_count.value += action_count

    @staticmethod
    def work_start(_work_obj: Worker) -> None:
        if os.path.exists(ADSFinder.FILE_NAME):
            os.remove(ADSFinder.FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker) -> None:
        return
