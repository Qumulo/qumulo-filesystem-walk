import argparse
import codecs
import hashlib
import io
import math
import os
import random
import zlib

from typing import Sequence, Union

from typing_extensions import TypedDict

from . import FileInfo, Worker


class Result(TypedDict):
    cf: Union[int, str]
    md5: str


class DataReductionTest:
    FILE_NAME = "data-reduction-test-results.txt"

    def __init__(self, in_args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        parser.add_argument("--perc", help="", dest="perc")
        args = parser.parse_args(in_args)
        self.sample_perc = 0.05
        if args.perc:
            self.sample_perc = float(args.perc)

    @staticmethod
    def process_it(
        work_obj: Worker,
        file_id: str,
        offset: int,
        md5: "hashlib._Hash",
    ) -> Result:
        fw = io.BytesIO()
        work_obj.rc.fs.read_file(file_=fw, id_=file_id, offset=offset, length=4096)
        fw.seek(0)
        c_len = len(zlib.compress(fw.read(), 4))
        fw.seek(0)
        md5.update(fw.read())
        b64 = codecs.encode(md5.digest(), "base64").decode()
        c_level = int(round(10 * c_len / 4096.0, 0))
        if c_level == 10:
            c_level = 9
        return {"cf": c_level, "md5": b64[0:10]}

    def every_batch(self, file_list: Sequence[FileInfo], work_obj: Worker) -> None:
        res = []
        action_count = 0
        md5 = hashlib.md5()
        for file_obj in file_list:
            if file_obj["type"] == "FS_FILE_TYPE_FILE":
                # sample 5% of files
                if random.random() < (1 - self.sample_perc):
                    continue
                action_count += 1
                file_size = int(file_obj["size"])
                md5 = hashlib.md5()
                try:
                    c_start = self.process_it(work_obj, file_obj["id"], 0, md5)
                except:
                    continue
                c_end: Result = {"cf": "X", "md5": "X"}
                c_middle: Result = {"cf": "X", "md5": "X"}
                if file_size > 4096 * 2:
                    try:
                        c_end = self.process_it(
                            work_obj, file_obj["id"], file_size - 4096, md5
                        )
                    except:
                        continue
                if file_size > 4096 * 3:
                    try:
                        c_middle = self.process_it(
                            work_obj,
                            file_obj["id"],
                            int(math.floor((file_size / 2.0) / 4096) * 4096),
                            md5,
                        )
                    except:
                        continue
                ext = file_obj["name"].rpartition(".")[-1]
                ext = ext.encode("ascii", "ignore").decode("ascii")
                if len(ext) > 6:
                    ext = ext[0:6]
                res.append(
                    "%s|%s|%s|%s|%s|%s|%s|%s"
                    % (
                        c_start["cf"],
                        c_middle["cf"],
                        c_end["cf"],
                        c_start["md5"],
                        c_middle["md5"],
                        c_end["md5"],
                        ext,
                        file_obj["size"],
                    )
                )
                if action_count >= 100:
                    with work_obj.result_file_lock:
                        work_obj.action_count.value += action_count
                    action_count = 0

        with work_obj.result_file_lock:
            with io.open(DataReductionTest.FILE_NAME, "a+", encoding="utf8") as f:
                for line in res:
                    f.write(line + "\n")
            work_obj.action_count.value += action_count

    @staticmethod
    def work_start(_work_obj: Worker) -> None:
        if os.path.exists(DataReductionTest.FILE_NAME):
            os.remove(DataReductionTest.FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker) -> None:
        return
