import os
import re
import sys
import time
import traceback
import multiprocessing
from qumulo.rest_client import RestClient
from qumulo.lib.request import RequestError
# Import all defined classes
from qtasks.ChangeExtension import *
from qtasks.DataReductionTest import *
from qtasks.ModeBitsChecker import *
from qtasks.Search import *
from qtasks.SummarizeOwners import *

try:
    import queue # python2/3
except:
    pass

MAX_QUEUE_LENGTH = 10000000
BATCH_SIZE = 2000
MAX_WORKER_COUNT = 96
if os.getenv('QWORKERS'):
    MAX_WORKER_COUNT = int(os.getenv('QWORKERS'))

def log_it(msg):
    print("%s: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg))
    sys.stdout.flush()


class QWalkWorker:
    # Thie class has gotten a bit too circular/interdependant with qtasks.py
    def get_counters(self):
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

    def __init__(self, creds, run_class, start_path, make_changes, log_file, counters=None):
        is_initialized = False
        try:
            if self.o_start_time:
                is_initialized = True
        except:
            pass

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
        self.run_class = run_class
        self.worker_id = None
        self.MAKE_CHANGES = make_changes
        self.LOG_FILE_NAME = log_file
        self.start_path = '/' if start_path == '/' else re.sub('/$', '', start_path)
        self.queue = multiprocessing.Queue()
        self.queue_lock = multiprocessing.Lock()
        self.count_lock = multiprocessing.Lock()
        self.write_file_lock = multiprocessing.Lock()
        self.result_file_lock = multiprocessing.Lock()
        self.start_time = time.time()
        self.rcs = self.get_many_clients(creds, MAX_WORKER_COUNT)
        self.rc = None
        self.pool = multiprocessing.Pool(MAX_WORKER_COUNT, 
                                         QWalkWorker.worker_main,
                                         (QWalkWorker.list_dir, self))

    def get_many_clients(self, creds, client_count):
        rc = RestClient(creds["QHOST"], 8000)
        rc.login(creds["QUSER"], creds["QPASS"])
        ips = []
        rcs = []
        for d in rc.network.list_network_status_v2(1):
            ips.append(d['network_statuses'][0]['address'])
        for i in range(0, client_count):
            rcc = RestClient(ips[i % len(ips)], 8000)
            rcc.login(creds["QUSER"], creds["QPASS"])
            rcs.append(rcc)
        return rcs

    def run(self):
        if not os.path.exists("old-queue.txt"):
            self.run_class.work_start(self)
            d_attr = self.rcs[0].fs.read_dir_aggregates(path=self.start_path, max_entries = 0)
            d_attr["total_directories"] = 1 + int(d_attr["total_directories"])
            d_attr["total_inodes"] = d_attr["total_directories"] + int(d_attr["total_files"])
            log_it("Walking - Dirs:%(total_directories)9s  Inodes:%(total_inodes)10s" % d_attr)
            self.add_to_queue({"path_id": d_attr['id']})
            self.wait_for_complete()
        else:
            with open("old-queue.txt", "r") as fr:
                last_time = time.time()
                for line in fr:
                    while self.queue_len.value > MAX_QUEUE_LENGTH:
                        if time.time() - last_time > 10:
                            self.print_status()
                            last_time = time.time()
                        time.sleep(1)
                    self.add_to_queue({"path_id": line})
                    if time.time() - last_time > 10:
                        self.print_status()
                        last_time = time.time()
            os.remove("old-queue.txt")
            self.wait_for_complete()
        self.pool.close()
        self.pool.join()
        self.queue.close()
        self.queue.join_thread()
        for rc in self.rcs:
            rc.close()
            del rc
        del self.pool
        del self.queue
        del self.rcs

    def print_status(self):
        log_it("Update  - Dirs:%9s  Inodes:%10s  Actions: %10s  Dirs/sec: %4s  Files/sec: %6s  Qlen:%8s" % (
                self.dir_count.value,
                self.file_count.value,
                self.action_count.value,
                int((self.dir_count.value-self.dir_counter) / (time.time() - self.start_time)),
                int((self.file_count.value-self.file_counter) / (time.time() - self.start_time)),
                self.queue_len.value,
                ))
        self.dir_counter = self.dir_count.value
        self.file_counter = self.file_count.value
        self.start_time = time.time()

    def add_to_queue(self, d):
        with self.queue_lock:
            self.queue_len.value += 1
            self.queue.put(d)

    def wait_for_complete(self, delay=10):
        time.sleep(2) # allow all worker processes to start.
        while True:
            self.print_status()
            time.sleep(delay)
            if self.queue_len.value <= 0 and self.active_workers.value <= 0:
                break
        log_it("Donestep- Dirs:%9s  Inodes:%10s  Actions: %10s  Dirs/sec: %4s  Files/sec: %6s" % (
                self.dir_count.value
                , self.file_count.value
                , self.action_count.value
                , int(self.dir_count.value / (time.time() - self.o_start_time))
                , int(self.file_count.value / (time.time() - self.o_start_time))
                ))

    @staticmethod
    def run_all(args, other_args = None):
        if other_args:
            run_class = eval(args.c)(other_args)
        else:
            run_class = eval(args.c)

        w = QWalkWorker({"QHOST": args.s, "QUSER": args.u, "QPASS": args.p}, 
                        run_class, 
                        args.d,   # starting directory
                        args.g,
                        args.l,
                        None,
                        )
        w.run()
        while os.path.exists("new-queue.txt"):
            os.rename("new-queue.txt", "old-queue.txt")
            counters = w.get_counters()
            del w
            w = QWalkWorker({"QHOST": args.s, "QUSER": args.u, "QPASS": args.p}, 
                            eval(args.c), 
                            args.d,   # starting directory
                            args.g,
                            args.l,
                            counters
                            )
            w.run()
        w.run_class.work_done(w)


    @staticmethod
    def worker_main(func, ww):
        p_name = multiprocessing.current_process().name
        ww.worker_id = int(re.match(r'.*?-([0-9])+', p_name).groups(1)[0])-1
        ww.rc = ww.rcs[ww.worker_id % MAX_WORKER_COUNT]
        file_list = []
        with ww.queue_lock:
            ww.active_workers.value += 1
        while True:
            try:
                data = ww.queue.get(True, timeout=5)
                file_list += func(data, ww)
                if len(file_list) >= BATCH_SIZE:
                    ww.run_class.every_batch(file_list, ww)
                    file_list = []
            except queue.Empty:
                # this is expected
                break
            except:
                # this is not expected
                log_it("!! Exception !!")
                log_it(sys.exc_info())
                traceback.print_exc(file=sys.stdout)
            with ww.queue_lock:
                ww.queue_len.value -= 1
        ww.run_class.every_batch(file_list, ww)
        with ww.queue_lock:
            ww.active_workers.value -= 1

    @staticmethod
    def list_dir(d, ww):
        file_count = 0
        next_uri = 'first'
        leftovers = []
        file_list = []
        while True:
            try:
                if next_uri == 'first':
                    res = ww.rc.fs.read_directory(id_=d['path_id'], page_size=1000)
                elif next_uri == 'directory_deleted':
                    break
                elif next_uri != '':
                    res = ww.rc.request('GET', next_uri)
                else:
                    break
            except RequestError as e:
                # handle API errors, usually it's the 10 hour expiration.
                time.sleep(5)
                if '404' in str(e):
                    next_uri = 'directory_deleted'
                else:
                    log_it("HTTP API error: %s" % re.sub(r'[\r\n]+', ' ', str(e))[:100])
                    log_it("id: %s - next_uri: %s" % (d['path_id'], next_uri))
                    ww.rc.login(ww.creds["QUSER"], ww.creds["QPASS"])
                continue
            except:
                print("EXCEPTION!")
                break
            for dd in res['files']:
                dd["dir_id"] = d['path_id']
                if dd['type'] == 'FS_FILE_TYPE_DIRECTORY':
                    if ww.queue_len.value > MAX_QUEUE_LENGTH:
                        leftovers.append(dd['id'])
                    else:
                        ww.add_to_queue({"path_id": dd['id']})
                file_count += 1
            file_list += res['files']
            if len(file_list) >= BATCH_SIZE:
                ww.run_class.every_batch(file_list, ww)
                with ww.count_lock:
                    ww.file_count.value += file_count
                file_count = 0
                file_list = []

            next_uri = res['paging']['next']
            if len(leftovers) > 0:
                with ww.write_file_lock:
                    fw = open("new-queue.txt", "a")
                    fw.write('\n'.join(leftovers))
                    fw.write('\n')
                    fw.close()
                    leftovers = []

        with ww.count_lock:
            ww.dir_count.value += 1
            ww.file_count.value += file_count
        return file_list