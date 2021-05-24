#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import os
import io
import sys
import argparse

os.environ["QWORKERS"] = "2"
os.environ["QWAITSECONDS"] = "5"

from typing import Sequence, Optional

from qwalk_worker import Creds, QWalkWorker, log_it
from qumulo.rest_client import RestClient
from qtasks.ChangeExtension import ChangeExtension
from qtasks.DataReductionTest import DataReductionTest
from qtasks.ModeBitsChecker import ModeBitsChecker
from qtasks.Search import Search
from qtasks.SummarizeOwners import SummarizeOwners
from qtasks.ApplyAcls import ApplyAcls
from qtasks.CopyDirectory import CopyDirectory


LOG_FILE_NAME = "test-qwalk-log-file.txt"


def read_full_tree_flat(rc: RestClient, path: str) -> Sequence[str]:
    items = []
    for d in rc.fs.tree_walk_preorder(path=path):
        items.append(d["name"])
    return sorted(items)


def test_search(
    creds: Creds, start_path: str, search: Sequence[str], snapshot: Optional[str] = None
) -> None:
    log_it("Search: %s" % search)
    w = QWalkWorker(
        creds, Search(search), start_path, snapshot, False, LOG_FILE_NAME, None
    )
    w.run()
    if os.path.exists(LOG_FILE_NAME):
        content = re.sub(r"[\r\n]+", " ", open(LOG_FILE_NAME).read())
        log_it("FOUND!  : Search found - %s" % content)
        os.remove(LOG_FILE_NAME)
    else:
        log_it("NOTFOUND: Search failure: %s" % search)


def main() -> None:
    parser = argparse.ArgumentParser(description="Test the qwalk.py script")
    parser.add_argument("-s", help="Qumulo hostname", required=True)
    parser.add_argument(
        "-u", help="Qumulo API user", default=os.getenv("QUSER") or "admin"
    )
    parser.add_argument(
        "-p", help="Qumulo API password", default=os.getenv("QPASS") or "admin"
    )
    parser.add_argument("-d", help="Test Directory", default="/test-qwalk-parent")

    try:
        args, _other_args = parser.parse_known_args()
    except:
        print("-" * 80)
        parser.print_help()
        print("-" * 80)
        sys.exit(0)

    # Everything will happen in a new subdirectory.
    test_dir_name = "test-qwalk"
    creds: Creds = {"QHOST": args.s, "QUSER": args.u, "QPASS": args.p}
    log_it("Log in to: %s" % (args.s))
    rc = RestClient(creds["QHOST"], 8000)
    rc.login(creds["QUSER"], creds["QPASS"])
    parent_dir = "/"
    if args.d != "/":
        parent_dir = re.sub(r"/$", "", args.d)
    log_it(
        "Create directory: %s/%s"
        % (parent_dir if parent_dir != "/" else "", test_dir_name)
    )
    test_dir = rc.fs.create_directory(dir_path=parent_dir, name=test_dir_name)
    args.d = "%s/%s" % (parent_dir, "test-qwalk")
    flowers_dir = rc.fs.create_directory(dir_path=args.d, name="flowers")
    foods_dir = rc.fs.create_directory(dir_path=args.d, name="foods")

    log_it("Create files")
    f = {}
    f["cat"] = rc.fs.create_file(dir_id=test_dir["id"], name="cat.txt")
    f["mouse"] = rc.fs.create_file(dir_id=test_dir["id"], name="mouse.jpg")
    f["dog"] = rc.fs.create_file(dir_id=test_dir["id"], name="dog.jpeg")
    f["bear"] = rc.fs.create_file(dir_id=test_dir["id"], name="bear.mov")
    f["rose"] = rc.fs.create_file(dir_id=flowers_dir["id"], name="rose.jpg")
    f["violet"] = rc.fs.create_file(dir_id=flowers_dir["id"], name="violet.jpg")
    f["cherry"] = rc.fs.create_file(dir_id=flowers_dir["id"], name="cherry.mpeg")
    f["pasta"] = rc.fs.create_file(dir_id=foods_dir["id"], name="pasta.txt")
    f["greenbeans"] = rc.fs.create_file(dir_id=foods_dir["id"], name="greenbeans.txt")
    f["rice"] = rc.fs.create_file(dir_id=foods_dir["id"], name="rice.txt")
    f["sushi"] = rc.fs.create_file(dir_id=foods_dir["id"], name="寿.漢")
    f["sushi_test"] = rc.fs.create_file(dir_id=foods_dir["id"], name="寿.test")
    rc.fs.set_file_attr(id_=f["greenbeans"]["id"], mode="0000")
    log_it("Write data to files")
    f_size = 1
    for _k, v in f.items():
        fw = io.BytesIO(b"0123456789" * f_size)
        fw.seek(0)
        rc.fs.write_file(data_file=fw, id_=v["id"])
        f_size *= 4
        fw.close()

    log_it("Test CopyDirectory")
    w = QWalkWorker(
        creds,
        CopyDirectory(["--to_dir", parent_dir + "/test-qwalk-copy"]),
        args.d,
        None,
        True,
        LOG_FILE_NAME,
        None,
    )
    w.run()
    items = read_full_tree_flat(rc, parent_dir + "/test-qwalk-copy")
    log_it("Copy item count: %s" % len(items))

    print("-" * 80)
    log_it("Test ApplyAcls")
    log_it("acls: %s" % len(rc.fs.get_acl(id_=f["pasta"]["id"])["acl"]["aces"]))
    w = QWalkWorker(
        creds,
        ApplyAcls(["--replace_acls", "examples/acls-everyone-all-access.json"]),
        foods_dir["path"],
        None,
        True,
        LOG_FILE_NAME,
        None,
    )
    w.run()
    log_it("acls: %s" % len(rc.fs.get_acl(id_=f["pasta"]["id"])["acl"]["aces"]))
    w = QWalkWorker(
        creds,
        ApplyAcls(["--add_entry", "examples/ace-everyone-read-only.json"]),
        foods_dir["path"],
        None,
        True,
        LOG_FILE_NAME,
        None,
    )
    w.run()
    log_it("acls: %s" % len(rc.fs.get_acl(id_=f["pasta"]["id"])["acl"]["aces"]))
    log_it("acls before : %s" % len(rc.fs.get_acl(id_=foods_dir["id"])["acl"]["aces"]))
    w = QWalkWorker(
        creds,
        ApplyAcls(
            [
                "--add_entry",
                "examples/ace-everyone-execute-traverse.json",
                "--dirs_only",
            ]
        ),
        test_dir["path"],
        None,
        True,
        LOG_FILE_NAME,
        None,
    )
    w.run()
    log_it("acls after: %s" % len(rc.fs.get_acl(id_=foods_dir["id"])["acl"]["aces"]))
    log_it("Done Test ApplyAcls")
    print("-" * 80)

    log_it("Test snapshot search after deleting file")
    snap = rc.snapshot.create_snapshot(name="test-qwalk", id_=test_dir["id"])
    rc.fs.delete(id_=f["pasta"]["id"])
    test_search(creds, args.d, ["--str", "pasta"], snap["id"])

    log_it("Test snapshot recover")
    w = QWalkWorker(
        creds,
        CopyDirectory(["--to_dir", parent_dir + "/copy-from-snap"]),
        args.d,
        snap["id"],
        True,
        LOG_FILE_NAME,
        None,
    )
    w.run()
    items = read_full_tree_flat(rc, parent_dir + "/copy-from-snap")
    log_it("Copy item count in snap: %s" % len(items))

    rc.snapshot.delete_snapshot(snap["id"])
    log_it("Deleted test snapshot")
    print("-" * 80)

    log_it("Start: DataReductionTest")
    w = QWalkWorker(
        creds,
        DataReductionTest(["--perc", "1"]),
        args.d,
        None,
        True,
        LOG_FILE_NAME,
        None,
    )
    w.run()
    print("." * 80)
    print(open(DataReductionTest.FILE_NAME).read().strip())
    print("." * 80)
    w.run_class.work_done(w)
    os.remove(DataReductionTest.FILE_NAME)
    log_it("Done with DataReductionTest")
    print("-" * 80)

    log_it("Start: ModeBitsChecker")
    rc.fs.set_file_attr(id_=f["greenbeans"]["id"], mode="0000")
    w = QWalkWorker(creds, ModeBitsChecker(), args.d, None, True, LOG_FILE_NAME, None)
    w.run()
    print("." * 80)
    print(open(ModeBitsChecker.FILE_NAME).read().strip())
    print("." * 80)
    w.run_class.work_done(w)
    os.remove(ModeBitsChecker.FILE_NAME)
    log_it("Done with ModeBitsChecker")
    print("-" * 80)

    log_it("Start: SummarizeOwners")
    w = QWalkWorker(creds, SummarizeOwners(), args.d, None, True, LOG_FILE_NAME, None)
    w.run()
    w.run_class.work_done(w)

    test_search(creds, args.d, ["--re", ".*jpeg"])
    log_it("Start: ChangeExtension: 'jpeg' to 'jpg'")
    w = QWalkWorker(
        creds,
        ChangeExtension(["--from", "jpeg", "--to", "jpg"]),
        args.d,
        None,
        True,
        LOG_FILE_NAME,
        None,
    )
    w.run()
    log_it("Done : ChangeExtension: 'jpeg' to 'jpg'")
    test_search(creds, args.d, ["--re", ".*jpeg"])
    print("-" * 80)
    test_search(creds, args.d, ["--re", ".*[.]txt"])
    print("-" * 80)
    test_search(creds, args.d, ["--str", "rose"])
    print("-" * 80)
    test_search(creds, args.d, ["--str", "pig"])
    print("-" * 80)

    log_it(
        "Copy tree file count: %s"
        % (
            rc.fs.read_dir_aggregates(
                path=parent_dir + "/test-qwalk-copy", max_entries=0
            )["total_files"]
        )
    )

    log_it(
        "Delete directory: %s/%s"
        % (parent_dir if parent_dir != "/" else "", test_dir_name)
    )
    rc.fs.delete_tree(id_=test_dir["id"])
    rc.fs.delete_tree(path=parent_dir + "/test-qwalk-copy")
    rc.fs.delete_tree(path=parent_dir + "/copy-from-snap")


if __name__ == "__main__":
    main()
