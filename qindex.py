#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import re
import argparse
import random
import time
from qwalk_worker import QWalkWorker,log_it
from qumulo.rest_client import RestClient
from qtasks.Search import *
import multiprocessing
try:
    import queue # python2/3
except:
    pass


SNAP_NAME = "Snapshot for index"
MAX_WORKER_COUNT = 96
if 'win' in sys.platform.lower():
    MAX_WORKER_COUNT = 60  # https://bugs.python.org/issue26903
if os.getenv('QWORKERS'):
    MAX_WORKER_COUNT = int(os.getenv('QWORKERS'))
WAIT_SECONDS = 5
if os.getenv('QWAITSECONDS'):
    WAIT_SECONDS = int(os.getenv('QWAITSECONDS'))


def log_item(rc, path_ids, op, snapshot_id, dir_id, item):
    parent_path = os.path.dirname(item["path"])
    if dir_id is None:
        if parent_path not in path_ids:
            d = rc.fs.get_file_attr(path = parent_path, snapshot = snapshot_id)
            path_ids[parent_path] = d["id"]
        dir_id = path_ids[parent_path]
    else:
        path_ids[parent_path] = dir_id
    line = "%(id)s|%(type)s|%(path)s|%(name)s|%(size)s|%(blocks)s|%(owner)s|%(change_time)s|" % item
    return "%s|%s|%s" % (dir_id, line, op)


def snap_worker(creds, q, q_lock, q_len, w_lock, w_file):
    p_name = multiprocessing.current_process().name
    worker_id = int(re.match(r'.*?-([0-9])+', p_name).groups(1)[0])-1
    rc = RestClient(creds["QHOST"], 8000)
    rc.login(creds["QUSER"], creds["QPASS"])
    file_list = []
    path_ids = {}

    while True:
        try:
            data = q.get(True, timeout=5)
            log_items = []
            for li in data["list"]:
                if li["op"] == "DELETE":
                    if "item" not in li:
                        old_item = rc.fs.get_file_attr(path = li["path"], snapshot = data["snap_before_id"])
                    else:
                        old_item = li["item"]
                    if old_item["type"] == "FS_FILE_TYPE_DIRECTORY":
                        log_items.append(log_item(rc, path_ids, "DELETE", data["snap_before_id"], li["dir_id"], old_item))
                        list_items = []
                        for dd in rc.fs.read_entire_directory(id_ = old_item["id"], 
                                                              snapshot = data["snap_before_id"]):
                            for item in dd["files"]:
                                if item["type"] == "FS_FILE_TYPE_DIRECTORY":
                                    list_items.append({"op": "DELETE", "item": item, "path": item["path"], "dir_id": old_item["id"]})
                                else:
                                    log_items.append(log_item(rc, path_ids, "DELETE", data["snap_before_id"], old_item["id"], item))
                        add_to_q(q, q_lock, q_len, {"list": list_items, 
                                                    "snap_before_id": data["snap_before_id"],
                                                    "snap_after_id": data["snap_after_id"]})

                    else:
                        log_items.append(log_item(rc, path_ids, "DELETE", data["snap_before_id"], li["dir_id"], old_item))
                    continue
                # item exists in new snapshot because it's not a delete
                if "item" not in li:
                    new_item = rc.fs.get_file_attr(path = li["path"], snapshot = data["snap_after_id"])
                else:
                    new_item = li["item"]
                if li["op"] == "CREATE" and new_item["type"] == "FS_FILE_TYPE_DIRECTORY":
                    log_items.append(log_item(rc, path_ids, "CREATE", data["snap_after_id"], li["dir_id"], new_item))
                    list_items = []
                    for dd in rc.fs.read_entire_directory(id_ = new_item["id"], 
                                                          snapshot = data["snap_after_id"]):
                        for item in dd["files"]:
                            if item["type"] == "FS_FILE_TYPE_DIRECTORY":
                                list_items.append({"op": "CREATE", "item": item, "path": item["path"], "dir_id": new_item["id"]})
                            else:
                                log_items.append(log_item(rc, path_ids, "CREATE", data["snap_after_id"], new_item["id"], item))
                    add_to_q(q, q_lock, q_len, {"list": list_items, 
                                                "snap_before_id": data["snap_before_id"],
                                                "snap_after_id": data["snap_after_id"]})
                elif li["op"] == "CREATE":
                    log_items.append(log_item(rc, path_ids, "CREATE", data["snap_after_id"], None, new_item))
                elif new_item["type"] == "FS_FILE_TYPE_DIRECTORY":
                    # TODO: nothing, I think
                    pass
                elif new_item["type"] != "FS_FILE_TYPE_DIRECTORY":
                    log_items.append(log_item(rc, path_ids, "MODIFY", data["snap_after_id"], None, new_item))
        except queue.Empty:
            # this is expected
            break
        except:
            # this is not expected
            log_it("!! Exception !!")
            log_it(sys.exc_info())
            traceback.print_exc(file=sys.stdout)
        with w_lock:
            fw = open(w_file, mode="a", encoding='utf-8')
            for li in log_items:
                fw.write(li + "\n")
            fw.close()
        log_items = []
        with q_lock:
            q_len.value -= 1


def add_to_q(q, q_lock, q_len, item):
    with q_lock:
        q_len.value += 1
        q.put(item)


def process_snap_diff(creds, path, snap_before_id, snap_after_id):
    q = multiprocessing.Queue()
    q_len = multiprocessing.Value("i", 0)
    q_lock = multiprocessing.Lock()
    w_lock = multiprocessing.Lock()
    w_file = "output-qumulo-fs-index-%s-change-log-%s-%s.txt" % (
                                        re.sub("[^a-z0-9]+", "_", path), 
                                        snap_before_id, snap_after_id)
    fw = open(w_file, mode="w", encoding='utf-8')
    fw.close()
    rc = RestClient(creds["QHOST"], 8000)
    rc.login(creds["QUSER"], creds["QPASS"])
    log_it("Run get_all_snapshot_tree_diff")
    results = rc.snapshot.get_all_snapshot_tree_diff(older_snap=snap_before_id, 
                                                     newer_snap=snap_after_id)
    log_it("Done get_all_snapshot_tree_diff.")
    log_it("Creating worker pool.")
    pool = multiprocessing.Pool(MAX_WORKER_COUNT, 
                                 snap_worker,
                                 (creds, q, q_lock, q_len, w_lock, w_file))
    log_it("Add items to queue.")
    ent_list = []
    for res in results:
        for ent in res['entries']:
            ent["dir_id"] = None
            ent_list.append(ent)
            if len(ent_list) > 5:
                add_to_q(q, q_lock, q_len, {"list": ent_list,
                                            "snap_before_id": snap_before_id,
                                            "snap_after_id": snap_after_id})
                ent_list = []
    add_to_q(q, q_lock, q_len, {"list": ent_list,
                                "snap_before_id": snap_before_id,
                                "snap_after_id": snap_after_id})
    log_it("Done adding items to queue.")
    while True:
        log_it("Queue length: %s" % q_len.value)
        time.sleep(WAIT_SECONDS)
        if q_len.value <= 0:
            break


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s', help='Qumulo hostname', required=True)
    parser.add_argument('-u', help='Qumulo API user', 
                              default=os.getenv('QUSER') or 'admin')
    parser.add_argument('-p', help='Qumulo API password',
                              default=os.getenv('QPASS') or 'admin')
    parser.add_argument('-d', help='Root Directory', default='/')
    try:
        args, other_args = parser.parse_known_args()
    except:
        print("-"*80)
        parser.print_help()
        print("-"*80)
        sys.exit(0)
    if args.d != '/':
        args.d = re.sub('/$', '', args.d) + '/'

    creds = {"QHOST": args.s, "QUSER": args.u, "QPASS": args.p}
    log_it("Log in to: %s" % (args.s))
    rc = RestClient(creds["QHOST"], 8000)
    rc.login(creds["QUSER"], creds["QPASS"])

    res = rc.snapshot.list_snapshot_statuses()
    snap_before = None
    snap_after = None
    for snap in sorted(res['entries'], key=lambda d: int(d['id'])):
        if snap['name'] == SNAP_NAME and snap['source_file_path'] == args.d:
            if snap_before is None:
                snap_before = snap
            elif snap_after is None:
                snap_after = snap

    if snap_before and not snap_after:
        snap_after = rc.snapshot.create_snapshot(path=args.d, name=SNAP_NAME)        
        log_it("Created new snapshot %s on %s" % (snap_after['id'], args.d))

    if snap_before and snap_after:
        log_it("Diffing snaps on %s between %s and %s" % (args.d, 
                                                      snap_before['timestamp'][0:16],
                                                      snap_after['timestamp'][0:16]))
        process_snap_diff(creds, 
                          args.d,
                          snap_before['id'],
                          snap_after['id']
                          )
        rc.snapshot.delete_snapshot(snapshot_id = snap_before['id'])
    else:
        # initial tree walk
        snap_before = rc.snapshot.create_snapshot(path=args.d, name=SNAP_NAME)
        log_it("Initial tree walk for: %s+snap:%s" % (args.d, snap_before['id']))
        log_file = "output-qumulo-fs-index-%s-tree.txt" % (re.sub("[^a-z0-9]+", "_", args.d))
        w = QWalkWorker(creds, 
                        Search(['--re', '.', 
                                '--cols', 'dir_id,id,type,path,name,size,blocks,owner,change_time,link_target,NEW']), 
                        args.d, str(snap_before['id']), None, log_file, None)
        w.run()


if __name__ == "__main__":
    main()
