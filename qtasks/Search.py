import argparse
import io
import os
import re

from typing import Sequence

from . import FileInfo, Worker


class Search:
    def __init__(self, in_args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        parser.add_argument("--re", help="", dest="search_re")
        parser.add_argument("--str", help="", dest="search_str")
        parser.add_argument("--itemtype", help="")
        parser.add_argument("--cols", help="")
        args = parser.parse_args(in_args)
        self.itemtype = None
        self.search_str = None
        self.search_re = None
        self.cols = ["path"]
        if args.search_re:
            self.search_re = re.compile(args.search_re, re.IGNORECASE)
        if args.search_str:
            self.search_str = args.search_str
        if args.cols:
            self.cols = args.cols.split(",")
        if args.itemtype:
            self.itemtype = args.itemtype

    def every_batch(self, file_list: Sequence[FileInfo], work_obj: Worker) -> None:
        results = []
        for file_obj in file_list:
            found = False
            if self.search_str:
                if self.search_str in file_obj["path"]:
                    found = True
            elif self.search_re:
                if self.search_re.match(file_obj["path"]):
                    found = True
            else:
                found = True

            if found:
                if "name" in self.cols:
                    file_obj["name"] = re.sub(r"[|\r\n\\]+", "", file_obj["name"])
                if "path" in self.cols:
                    file_obj["path"] = re.sub(r"[|\r\n\\]+", "", file_obj["path"])
                file_obj["link_target"] = ""
                if "link_target" in self.cols and "link" in file_obj["type"].lower():
                    try:
                        with io.BytesIO() as f_bin:
                            work_obj.rc.fs.read_file(id_=file_obj["id"], file_=f_bin)
                            f_bin.seek(0)
                            target = f_bin.read().decode("utf8").strip().strip("\x00")

                        parent_path = os.path.dirname(file_obj["path"])
                        file_obj["link_target"] = re.sub(
                            r"[|\r\n\\]+",
                            "",
                            os.path.normpath(os.path.join(parent_path, target)),
                        )
                    except:
                        pass
                if self.itemtype is None or self.itemtype in file_obj["type"].lower():
                    line = "|".join(
                        [
                            # mypy insists "TypedDict key must be a string literal"
                            file_obj[col] if col in file_obj else col  # type: ignore[misc]
                            for col in self.cols
                        ]
                    )
                    results.append(line)

        if len(results) > 0:
            with work_obj.result_file_lock:
                with io.open(work_obj.LOG_FILE_NAME, "a", encoding="utf8") as f_txt:
                    for d in results:
                        f_txt.write("%s\n" % d)
                work_obj.action_count.value += len(results)

    @staticmethod
    def work_start(work_obj: Worker) -> None:
        if os.path.exists(work_obj.LOG_FILE_NAME):
            os.remove(work_obj.LOG_FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker) -> None:
        pass
