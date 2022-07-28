import argparse
import io
import os
import sys
import time
import traceback

from typing import Dict, Optional, Sequence, List

from qumulo.rest_client import RestClient

from . import FileInfo, Worker

DEBUG = False
if os.getenv("QDEBUG"):
    DEBUG = True


def log_it(msg: str) -> None:
    if DEBUG:
        print("%s: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg))
        sys.stdout.flush()


class CopyDirectory:
    def __init__(self, in_args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        parser.add_argument("--to_dir", help="destination directory")
        parser.add_argument(
            "--skip_hardlinks", help=" skip hard links", action="store_true"
        )
        parser.add_argument(
            "--no_preserve",
            help="will not preserve permissions or timestamps",
            action="store_true",
        )
        args = parser.parse_args(in_args)
        self.to_dir: Optional[str] = None
        self.skip_hardlinks: Optional[bool] = None
        self.no_preserve: Optional[bool] = None
        self.cols = ["path"]
        if args.to_dir:
            self.to_dir = args.to_dir
        if args.skip_hardlinks:
            self.skip_hardlinks = args.skip_hardlinks
        if args.no_preserve:
            self.no_preserve = args.no_preserve
        self.folders: Dict[str, str] = {}

    def create_folder(self, rc: RestClient, path: str) -> str:
        if path in self.folders:
            return self.folders[path]
        levels = path.split("/")
        for level in range(2, len(levels) + 1):
            new_dir = "/".join(levels[0:level])
            if new_dir not in self.folders:
                try:
                    new_f = rc.fs.get_file_attr(path=new_dir)
                    self.folders[new_dir] = new_f["id"]
                    continue
                except:
                    e_str = "get_file_attr exception creating directory: %s %s" % (
                        sys.exc_info(),
                        traceback.format_exc().replace("\n", ""),
                    )
                    if "fs_no_such_entry_error" not in e_str:
                        log_it(e_str)
                dir_path = "/".join(levels[0 : level - 1])
                if dir_path == "":
                    dir_path = "/"
                dir_name = levels[level - 1]
                try:
                    new_f = rc.fs.create_directory(dir_path=dir_path, name=dir_name)
                except:
                    log_it(
                        "create_directory exception: %s %s/%s"
                        % (sys.exc_info()[0], dir_path, dir_name)
                    )
                    new_f = rc.fs.get_file_attr(path=new_dir)
                    self.folders[new_dir] = new_f["id"]

                self.folders[new_dir] = new_f["id"]
        return self.folders[path]

    def every_batch(self, file_list: Sequence[FileInfo], work_obj: Worker) -> None:
        requeue: List[FileInfo] = []
        results = []
        for file_obj in file_list:
            try:
                to_path = file_obj["path"]
                if self.to_dir is not None:
                    to_path = to_path.replace(work_obj.start_path, self.to_dir)

                parent_path = os.path.dirname(to_path)
                file_name = os.path.basename(to_path)
                self.create_folder(work_obj.rc, parent_path)
                if file_obj["type"] == "FS_FILE_TYPE_DIRECTORY":
                    self.create_folder(work_obj.rc, to_path)

                    if not self.no_preserve:
                        new_f = work_obj.rc.fs.get_file_attr(path=to_path)
                        file_exists = new_f['id']

                        if new_f['child_count'] != file_obj['child_count']:
                            # We can't apply directory attributes until all children have been
                            # added since adding children updates timestamps
                            requeue.append(file_obj)
                            continue

                        o_attr = work_obj.rc.fs.get_file_attr(
                            snapshot = work_obj.snap,
                            id_ = file_obj["id"]
                        )
                        o_acl = work_obj.rc.fs.get_acl_v2(
                            snapshot = work_obj.snap,
                            id_ = file_obj["id"]
                        )

                        work_obj.rc.fs.set_file_attr(
                            id_ = file_exists,
                            owner=o_attr['owner'],
                            group=o_attr['group'],
                            extended_attributes=o_attr['extended_attributes'],
                        )
                        work_obj.rc.fs.set_acl_v2(
                            id_ = file_exists,
                            acl = o_acl
                        )
                        work_obj.rc.fs.set_file_attr(
                            id_ = file_exists,
                            creation_time = o_attr['creation_time'],
                            modification_time = o_attr['modification_time'],
                            change_time = o_attr['change_time'],
                        )

                    results.append("DIRECTORY : %s -> %s" % (file_obj["path"], to_path))
                else:
                    if file_obj["num_links"] > 1 and self.skip_hardlinks:
                        # skip any hard links
                        log_it("Skip hard link: %s" % file_obj["name"])
                        results.append("HARD LINK SKIPPED: %s" % file_obj["path"])
                        continue

                    if file_obj["num_links"] > 1:
                        results.append("HARD LINK FOUND: %s" % file_obj["path"])
                    file_exists = None
                    try:
                        new_f = work_obj.rc.fs.get_file_attr(path=to_path)
                        if (
                            new_f["size"] == file_obj["size"]
                            and new_f["modification_time"]
                            == file_obj["modification_time"]
                            and new_f["change_time"] == file_obj["change_time"]
                        ):
                            log_it(
                                "File exists with same size and timestamp: %s" % to_path
                            )
                            results.append(
                                "FILE EXISTS: %s -> %s" % (file_obj["path"], to_path)
                            )
                            file_exists = new_f["id"]
                        else:
                            print(
                                "%s != %s"
                                % (
                                    new_f["modification_time"],
                                    file_obj["modification_time"],
                                )
                            )
                            print(
                                "%s != %s"
                                % (new_f["change_time"], file_obj["change_time"])
                            )
                            work_obj.rc.fs.delete(id_=new_f["id"])
                    except:
                        if "fs_no_such_entry_error" not in str(sys.exc_info()[1]):
                            e_str = "get_file_attr exception: %s %s" % (
                                sys.exc_info(),
                                traceback.format_exc().replace("\n", ""),
                            )
                            log_it(e_str)
                            results.append(
                                "!!FILE COPY FAILED1: %s -> %s"
                                % (file_obj["path"], to_path)
                            )
                    try:
                        if not file_exists:
                            if file_obj["type"] == "FS_FILE_TYPE_SYMLINK":
                                log_it("Creating symlink: %s" % to_path)
                                with io.BytesIO() as link_f:
                                    work_obj.rc.fs.read_file(
                                        snapshot=work_obj.snap,
                                        id_=file_obj["id"],
                                        file_=link_f,
                                    )
                                    link_f.seek(0)
                                    target = bytes.decode(link_f.read()).replace(
                                        "\x00", ""
                                    )
                                new_f = work_obj.rc.fs.create_symlink(
                                    target=target, dir_path=parent_path, name=file_name
                                )
                                link_f.close()
                            else:
                                log_it("Creating file: %s" % to_path)
                                new_f = work_obj.rc.fs.create_file(
                                    dir_path=parent_path, name=file_name
                                )
                                work_obj.rc.fs.copy(
                                    source_id=file_obj["id"],
                                    target_path=to_path,
                                    source_snapshot=work_obj.snap,
                                )
                                results.append(
                                    "FILE COPIED: %s -> %s"
                                    % (file_obj["path"], to_path)
                                )
                            file_exists = new_f["id"]

                        o_attr = work_obj.rc.fs.get_file_attr(
                            snapshot=work_obj.snap, id_=file_obj["id"]
                        )
                        o_acl = work_obj.rc.fs.get_acl_v2(
                            snapshot=work_obj.snap, id_=file_obj["id"]
                        )

                        try:
                            for stream in work_obj.rc.fs.list_named_streams(
                                snapshot=work_obj.snap, id_=file_obj["id"]
                            ):
                                log_it(
                                    "Create stream: %s - %s"
                                    % (file_exists, stream["name"])
                                )
                                new_st = work_obj.rc.fs.create_stream(
                                    id_=file_exists, stream_name=stream["name"]
                                )
                                log_it(
                                    "Copy stream: %s:%s:%s -> %s:%s"
                                    % (
                                        work_obj.snap,
                                        file_obj["id"],
                                        stream["id"],
                                        file_exists,
                                        new_st["id"],
                                    )
                                )
                                work_obj.rc.fs.copy(
                                    source_snapshot=work_obj.snap,
                                    source_id=file_obj["id"],
                                    source_stream_id=stream["id"],
                                    target_id=file_exists,
                                    target_stream_id=new_st["id"],
                                )
                        except:
                            e_str = "Stream create/copy exception: %s %s" % (
                                sys.exc_info(),
                                traceback.format_exc().replace("\n", ""),
                            )
                            if (
                                "fs_invalid_file_type_error" not in e_str
                                and "fs_entry_exists_error" not in e_str
                            ):
                                log_it(e_str)
                        if not self.no_preserve:
                            work_obj.rc.fs.set_file_attr(
                                id_=file_exists,
                                owner=o_attr["owner"],
                                group=o_attr["group"],
                                extended_attributes=o_attr["extended_attributes"],
                            )
                            work_obj.rc.fs.set_acl_v2(id_=file_exists, acl=o_acl)
                            work_obj.rc.fs.set_file_attr(
                                id_=file_exists,
                                creation_time=o_attr["creation_time"],
                                modification_time=o_attr["modification_time"],
                                change_time=o_attr["change_time"],
                            )
                    except:
                        log_it(
                            "File create/copy exception: %s %s"
                            % (sys.exc_info(), traceback.format_exc())
                        )
                        results.append(
                            "!!FILE COPY FAILED2: %s -> %s"
                            % (file_obj["path"], to_path)
                        )
            except:
                log_it("Other exception: %s %s" % (sys.exc_info(), file_obj["path"]))
                results.append(
                    "!!FILE COPY FAILED3: %s -> %s" % (sys.exc_info(), file_obj["path"])
                )

        try:
            if len(results) > 0:
                with work_obj.result_file_lock:
                    with io.open(work_obj.LOG_FILE_NAME, "a", encoding="utf8") as f:
                        for d in results:
                            f.write("%s\n" % d)
                    work_obj.action_count.value += len(results)
        except:
            log_it("Unable to save results exception: %s" % str(sys.exc_info()))

        if len(requeue) > 0:
            if work_obj.queue_len.value < 50:
                # Add a slight delay so workers have time to add the necessary children
                time.sleep(0.1)
            work_obj.add_to_queue({"type": "process_list", "list": requeue})

    @staticmethod
    def work_start(work_obj: Worker) -> None:
        if os.path.exists(work_obj.LOG_FILE_NAME):
            os.remove(work_obj.LOG_FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker) -> None:
        pass
