import argparse
import io
import os

from typing import Dict, Sequence

from typing_extensions import TypedDict

from . import FileInfo, Worker


class OwnerInfo(TypedDict):
    owner: str
    id_type: str
    id_value: str
    count: int
    size: int


class SummarizeOwners:
    # A temporary file for storing the intermediate walk work
    FILE_NAME = "owners.txt"

    def __init__(self, in_args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        _args = parser.parse_args(in_args)

    def every_batch(  # pylint: disable=no-self-use
        self, file_list: Sequence[FileInfo], work_obj: Worker
    ) -> None:
        owners = {}
        for file_obj in file_list:
            k = (
                file_obj["owner"]
                + "|%(id_type)s|%(id_value)s" % file_obj["owner_details"]
            )
            if k not in owners:
                owners[k] = {"count": 1, "size": int(file_obj["size"])}
            else:
                owners[k]["count"] += 1
                owners[k]["size"] += int(file_obj["size"])

        with work_obj.result_file_lock:
            with io.open(SummarizeOwners.FILE_NAME, "a", encoding="utf8") as f:
                for k, v in owners.items():
                    f.write("%s|%s|%s\n" % (k, v["count"], v["size"]))
            work_obj.action_count.value += 1

    @staticmethod
    def work_done(_work_obj: Worker) -> None:
        print("-" * 80)
        owners: Dict[str, OwnerInfo] = {}
        with io.open(SummarizeOwners.FILE_NAME, "r", encoding="utf8") as f:
            for line in f:
                (owner, id_type, id_value, count_raw, size_raw) = line.split("|")
                count = int(count_raw)
                size = int(size_raw)
                if owner not in owners:
                    owners[owner] = {
                        "owner": owner,
                        "id_type": id_type,
                        "id_value": id_value,
                        "count": count,
                        "size": size,
                    }
                else:
                    owners[owner]["count"] += count
                    owners[owner]["size"] += size

        for _k, v in owners.items():
            print(
                "%(owner)12s (%(id_type)10s/%(id_value)48s): %(count)9s / %(size)15s"
                % v
            )
        os.remove(SummarizeOwners.FILE_NAME)
        print("-" * 80)

    @staticmethod
    def work_start(_work_obj: Worker) -> None:
        if os.path.exists(SummarizeOwners.FILE_NAME):
            os.remove(SummarizeOwners.FILE_NAME)
