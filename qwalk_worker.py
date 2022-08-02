import multiprocessing
import os
import pickle
import queue
import random
import re
import sys
import time
import traceback

from typing import Callable, cast, List, Mapping, Optional, Sequence, Type, Union

from typing_extensions import Literal, TypedDict

from qtasks import Task
from qtasks.ApplyAcls import ApplyAcls

# Import all defined classes
from qtasks.ChangeExtension import ChangeExtension
from qtasks.CopyDirectory import CopyDirectory
from qtasks.DataReductionTest import DataReductionTest
from qtasks.ModeBitsChecker import ModeBitsChecker
from qtasks.Search import Search
from qtasks.SummarizeOwners import SummarizeOwners
from qumulo.lib.request import RequestError
from qumulo.rest_client import RestClient

QTASKS: Mapping[str, Type[Task]] = {
    "ChangeExtension": ChangeExtension,
    "DataReductionTest": DataReductionTest,
    "ModeBitsChecker": ModeBitsChecker,
    "Search": Search,
    "SummarizeOwners": SummarizeOwners,
    "ApplyAcls": ApplyAcls,
    "CopyDirectory": CopyDirectory,
}


REST_PORT = 8000
USE_PICKLE = False
MAX_QUEUE_LENGTH = 100000
BATCH_SIZE = 100
MAX_WORKER_COUNT = 10
WAIT_SECONDS = 10
OVERRIDE_IPS = None
DEBUG = False

_QBATCHSIZE = os.getenv("QBATCHSIZE")
if _QBATCHSIZE:
    BATCH_SIZE = int(_QBATCHSIZE)
_QWORKERS = os.getenv("QWORKERS")
if _QWORKERS:
    MAX_WORKER_COUNT = int(_QWORKERS)
_QWAITSECONDS = os.getenv("QWAITSECONDS")
if _QWAITSECONDS:
    WAIT_SECONDS = int(_QWAITSECONDS)
if os.getenv("QUSEPICKLE"):
    USE_PICKLE = True
_QMAXLEN = os.getenv("QMAXLEN")
if _QMAXLEN:
    MAX_QUEUE_LENGTH = int(_QMAXLEN)
_QOVERRIDEIPS = os.getenv("QOVERRIDEIPS")
if _QOVERRIDEIPS:
    OVERRIDE_IPS = _QOVERRIDEIPS
if os.getenv("QDEBUG"):
    DEBUG = True


LOG_LOCK = multiprocessing.Lock()


def log_it(msg: str) -> None:
    print("%s: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg))
    sys.stdout.flush()


def log_exception(msg: str) -> None:
    global LOG_LOCK  # pylint: disable=global-statement
    if DEBUG:
        with LOG_LOCK:
            log_it(msg.replace("\n", ""))
            log_it(str(sys.exc_info()[0]).replace("\n", ""))
            s = traceback.format_exc()
            for line in s.split("\n"):
                log_it(line)


class Creds(TypedDict):
    QHOST: str
    QUSER: str
    QPASS: str
    QPORT: int


class Counters(TypedDict):
    o_start_time: float
    dir_counter: int
    file_counter: int
    queue_len: int
    action_count: int
    active_workers: int
    dir_count: int
    file_count: int


class ProcessListArgs(TypedDict):
    type: Literal["process_list"]
    list: Union[str, List[str]]


class ListDirArgs(TypedDict):
    type: Literal["list_dir"]
    path_id: str
    snapshot: Optional[str]


class QWalkWorker:  # pylint: disable=too-many-instance-attributes
    # The class has gotten a bit too circular/interdependant with qtasks.py
    def get_counters(self) -> Counters:
        return {
            "o_start_time": self.o_start_time,
            "dir_counter": self.dir_counter,
            "file_counter": self.file_counter,
            "queue_len": self.queue_len.value,
            "action_count": self.action_count.value,
            "active_workers": self.active_workers.value,
            "dir_count": self.dir_count.value,
            "file_count": self.file_count.value,
        }

    def __init__(  # pylint: disable=too-many-arguments
        self,
        creds: Creds,
        run_task: Task,
        start_path: str,
        snap: Optional[str],
        make_changes: bool,
        log_file: str,
        counters: Optional[Counters] = None,
    ):
        self.snap = snap
        self.o_start_time = time.time()
        self.dir_counter = 0
        self.file_counter = 0
        self.queue_len = multiprocessing.Value("i", 0)
        self.action_count = multiprocessing.Value("i", 0)
        self.active_workers = multiprocessing.Value("i", 0)
        self.dir_count = multiprocessing.Value("i", 0)
        self.file_count = multiprocessing.Value("i", 0)

        if counters:
            self.o_start_time = counters["o_start_time"]
            self.dir_counter = counters["dir_counter"]
            self.file_counter = counters["file_counter"]
            self.queue_len.value = counters["queue_len"]
            self.action_count.value = counters["action_count"]
            self.active_workers.value = counters["active_workers"]
            self.dir_count.value = counters["dir_count"]
            self.file_count.value = counters["file_count"]

        self.creds = creds
        self.run_task = run_task
        self.worker_id: Optional[int] = None
        self.MAKE_CHANGES = make_changes
        self.LOG_FILE_NAME = log_file
        self.start_path = "/" if start_path == "/" else re.sub("/$", "", start_path)
        self.queue: multiprocessing.Queue[  # pylint: disable=unsubscriptable-object
            Union[ProcessListArgs, ListDirArgs]
        ] = multiprocessing.Queue()
        self.queue_lock = multiprocessing.Lock()
        self.count_lock = multiprocessing.Lock()
        self.write_file_lock = multiprocessing.Lock()
        self.result_file_lock = multiprocessing.Lock()
        self.start_time = time.time()
        self.rc: RestClient = None
        if OVERRIDE_IPS is None:
            self.ips = self.rc_get_ips(self.creds)
        else:
            self.ips = re.split(r"[ ,]+", OVERRIDE_IPS)
        log_it("Using the following Qumulo IPS: %s" % ",".join(self.ips))
        self.pool = multiprocessing.Pool(  # pylint: disable=consider-using-with
            MAX_WORKER_COUNT, QWalkWorker.worker_main, (QWalkWorker.list_dir, self)
        )

    @staticmethod
    def rc_get_ips(creds: Creds) -> Sequence[str]:
        rc = RestClient(creds["QHOST"], creds.get("QPORT", REST_PORT))
        rc.login(creds["QUSER"], creds["QPASS"])
        ips = []
        for d in rc.network.list_network_status_v2(1):
            ips.append(d["network_statuses"][0]["address"])
        return ips

    def run(self) -> None:
        if not os.path.exists("old-queue.txt"):
            self.run_task.work_start(self)
            rc = RestClient(self.creds["QHOST"], self.creds.get("QPORT", REST_PORT))
            rc.login(self.creds["QUSER"], self.creds["QPASS"])
            if self.snap:
                d_attr = rc.fs.read_dir_aggregates(
                    path=self.start_path, snapshot=self.snap, max_entries=0
                )
            else:
                d_attr = rc.fs.read_dir_aggregates(path=self.start_path, max_entries=0)
            d_attr["total_directories"] = 1 + int(d_attr["total_directories"])
            d_attr["total_inodes"] = d_attr["total_directories"] + int(
                d_attr["total_files"]
            )
            log_it(
                "Walking - %(total_directories)9s dir|%(total_inodes)10s inod" % d_attr
            )
            self.add_to_queue(
                {"type": "list_dir", "path_id": d_attr["id"], "snapshot": self.snap}
            )
            self.wait_for_complete()
        else:
            with open("old-queue.txt", "r") as fr:
                last_time = time.time()
                for line in fr:
                    # back off because we have a lot of directories now
                    while self.queue_len.value > MAX_QUEUE_LENGTH:
                        if time.time() - last_time >= WAIT_SECONDS:
                            self.print_status()
                            last_time = time.time()
                        time.sleep(1)
                    self.add_to_queue(
                        {
                            "type": "list_dir",
                            "path_id": line.strip(),
                            "snapshot": self.snap,
                        }
                    )
                    if time.time() - last_time >= WAIT_SECONDS:
                        self.print_status()
                        last_time = time.time()
            os.remove("old-queue.txt")
            self.wait_for_complete()
        self.pool.close()
        self.pool.join()
        self.queue.close()
        self.queue.join_thread()
        if self.rc:
            rc.close()
            del rc
        del self.pool
        del self.queue

    def print_status(self) -> None:
        log_it(
            "Update  - %9s dir|%10s inod|%10s actn|%4s dir/s|%6s fil/s|%8s q"
            % (
                self.dir_count.value,
                self.file_count.value,
                self.action_count.value,
                int(
                    (self.dir_count.value - self.dir_counter)
                    / (time.time() - self.start_time)
                ),
                int(
                    (self.file_count.value - self.file_counter)
                    / (time.time() - self.start_time)
                ),
                self.queue_len.value,
            )
        )
        self.dir_counter = self.dir_count.value
        self.file_counter = self.file_count.value
        self.start_time = time.time()

    def add_to_queue(self, d: Union[ProcessListArgs, ListDirArgs]) -> None:
        with self.queue_lock:
            self.queue_len.value += 1
            self.queue.put(d)

    def wait_for_complete(self) -> None:
        time.sleep(0.5)  # allow all worker processes to start.
        while True:
            self.print_status()
            time.sleep(WAIT_SECONDS)
            if self.queue_len.value <= 0 and self.active_workers.value <= 0:
                break

        log_it(
            "Donestep- %9s dir|%10s inod|%10s actn|%4s dir/s|%6s fil/s"
            % (
                self.dir_count.value,
                self.file_count.value,
                self.action_count.value,
                int(self.dir_count.value / (time.time() - self.o_start_time)),
                int(self.file_count.value / (time.time() - self.o_start_time)),
            )
        )

    @staticmethod
    def run_all(  # pylint: disable=too-many-arguments
        hostname: str,
        username: str,
        password: str,
        start_dir: str,
        make_changes: bool,
        log_file: str,
        run_class_name: str,
        snapshot_id: Optional[str],
        other_args: Sequence[str],
    ) -> None:
        run_class = QTASKS[run_class_name]
        run_task = run_class(other_args)
        w = QWalkWorker(
            {"QHOST": hostname, "QUSER": username, "QPASS": password},
            run_task,
            start_dir,
            snapshot_id,
            make_changes,
            log_file,
            None,
        )
        w.run()
        while os.path.exists("new-queue.txt"):
            os.rename("new-queue.txt", "old-queue.txt")
            counters = w.get_counters()
            del w
            w = QWalkWorker(
                {"QHOST": hostname, "QUSER": username, "QPASS": password},
                run_task,
                start_dir,
                snapshot_id,
                make_changes,
                log_file,
                counters,
            )
            w.run()
        w.run_task.work_done(w)

    def queue_files(self, process_list: List[str]) -> List[str]:
        if len(process_list) > 0:
            if USE_PICKLE:
                filename_1 = "%s-%s.pkl" % (
                    time.time(),
                    random.random(),
                )
                with open(filename_1, "wb") as fw:
                    pickle.dump(process_list, fw)
                the_list_1: Union[str, List[str]] = filename_1
            else:
                the_list_1 = process_list
            self.add_to_queue(
                {"type": "process_list", "list": the_list_1}
            )
        return []

    @staticmethod
    def worker_main(  # pylint: disable=too-many-nested-blocks
        func: Callable[[ListDirArgs, "QWalkWorker"], Sequence[str]], ww: "QWalkWorker"
    ) -> None:
        p_name = multiprocessing.current_process().name
        match = re.match(r".*?-([0-9])+", p_name)
        assert match, p_name
        ww.worker_id = int(match.group(1)) - 1
        rc = RestClient(random.choice(ww.ips), ww.creds.get("QPORT", REST_PORT))
        rc.login(ww.creds["QUSER"], ww.creds["QPASS"])
        client_start = time.time()
        ww.rc = rc
        file_list: List[str] = []
        with ww.queue_lock:
            ww.active_workers.value += 1
        process_list = []
        while True:
            if time.time() - client_start > 60 * 60:
                # re-initialize rest client every hour
                ww.rc.login(ww.creds["QUSER"], ww.creds["QPASS"])
                # log_it("re-initialized Qumulo rest client for worker")
            try:
                data = ww.queue.get(True, timeout=5)
                if data["type"] == "list_dir":
                    file_list += func(data, ww)
                    while len(file_list) > 0:
                        process_list.append(file_list.pop())
                        if len(process_list) >= BATCH_SIZE:
                            process_list = ww.queue_files(process_list)

                    # Queue a partial batch if we have it
                    process_list = ww.queue_files(process_list)

                elif data["type"] == "process_list":
                    if USE_PICKLE:
                        # TODO: instead of USE_PICKLE, infer from list type?
                        filename_2 = cast(str, data["list"])
                        with open(filename_2, "rb") as fr:
                            the_list_2 = pickle.load(fr)
                        os.remove(filename_2)
                    else:
                        the_list_2 = data["list"]
                    ww.run_task.every_batch(the_list_2, ww)
                    # the_list_2 = None
                with ww.queue_lock:
                    ww.queue_len.value -= 1
            except queue.Empty:
                try:
                    if len(process_list) > 0:
                        # log_exception("Queue empty, process_list > 0")
                        if USE_PICKLE:
                            filename_3 = "%s-%s.pkl" % (time.time(), random.random())
                            with open(filename_3, "wb") as fw:
                                pickle.dump(process_list, fw)
                            the_list_3: Union[str, List[str]] = filename_3
                        else:
                            the_list_3 = process_list
                        ww.add_to_queue({"type": "process_list", "list": the_list_3})
                        process_list = []
                        # the_list_3 = None
                    elif ww.queue_len.value > 0:
                        # log_exception("Queue empty exception, but queue length = %s." % (ww.queue_len.value))
                        pass
                    else:
                        # log_exception("Queue empty, process_list empty. No more work.")
                        break
                except:
                    log_exception("Queue empty process_list exception")
                    break
            except:
                # this is not expected
                log_exception("Exception in worker process")
        with ww.queue_lock:
            ww.active_workers.value -= 1

    @staticmethod
    def list_dir(d: ListDirArgs, ww: "QWalkWorker") -> Sequence[str]:
        file_count = 0
        next_uri = "first"
        leftovers = []
        file_list = []
        while True:
            try:
                if next_uri == "first":
                    if d["snapshot"] is not None:
                        res = ww.rc.fs.read_directory(
                            id_=d["path_id"], snapshot=d["snapshot"], page_size=100
                        )
                    else:
                        res = ww.rc.fs.read_directory(id_=d["path_id"], page_size=100)
                elif next_uri == "directory_deleted":
                    break
                elif next_uri != "":
                    res = ww.rc.request("GET", next_uri)
                else:
                    break
            except RequestError as e:
                # handle API errors, usually it's the 10 hour expiration or directory deleted.
                if "404" in str(e):
                    next_uri = "directory_deleted"
                else:
                    time.sleep(5)
                    log_it("HTTP API error: %s" % re.sub(r"[\r\n]+", " ", str(e))[:100])
                    log_it("id: %s - next_uri: %s" % (d["path_id"], next_uri))
                    ww.rc.login(ww.creds["QUSER"], ww.creds["QPASS"])
                continue
            except:
                log_exception("UNHANDLED EXCEPTION! - Stop reading directory")
                break
            try:
                dd = None
                for dd in res["files"]:
                    dd["dir_id"] = d["path_id"]
                    if dd["type"] == "FS_FILE_TYPE_DIRECTORY":
                        if ww.queue_len.value > MAX_QUEUE_LENGTH:
                            leftovers.append(dd["id"])
                        else:
                            ww.add_to_queue(
                                {
                                    "type": "list_dir",
                                    "path_id": dd["id"],
                                    "snapshot": d["snapshot"],
                                }
                            )
                    file_count += 1
                file_list += res["files"]
            except:
                log_exception("UNHANDLED EXCEPTION reading directory entries")

            # handle very large directories dynamically
            try:
                if len(file_list) >= BATCH_SIZE:
                    process_list = []
                    while len(file_list) > 0:
                        process_list.append(file_list.pop())
                        if len(process_list) >= BATCH_SIZE or len(file_list) == 0:
                            if USE_PICKLE:
                                filename = "%s-%s.pkl" % (time.time(), random.random())
                                with open(filename, "wb") as fw:
                                    pickle.dump(process_list, fw)
                                the_list: Union[str, List[str]] = filename
                            else:
                                the_list = process_list
                            ww.add_to_queue({"type": "process_list", "list": the_list})
                            process_list = []
                            # the_list = None

                    with ww.count_lock:
                        ww.file_count.value += file_count
                    file_count = 0
                    file_list = []
            except:
                log_exception(
                    "UNHANDLED EXCEPTION while working with a large directory"
                )

            try:
                next_uri = res["paging"]["next"]
                if len(leftovers) > 0:
                    with ww.write_file_lock:
                        with open("new-queue.txt", "a") as f:
                            f.write("\n".join(leftovers))
                            f.write("\n")
                        leftovers = []
            except:
                log_exception("UNHANDLED EXCEPTION handling leftover directory entries")

        with ww.count_lock:
            ww.dir_count.value += 1
            ww.file_count.value += file_count
        return file_list
