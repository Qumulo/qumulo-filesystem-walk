#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import os
import io
import sys
import time
import argparse
from qwalk_worker import QWalkWorker, log_it
from qumulo.rest_client import RestClient
from qtasks.ChangeExtension import *
from qtasks.DataReductionTest import *
from qtasks.ModeBitsChecker import *
from qtasks.Search import *
from qtasks.SummarizeOwners import *
from qtasks.ApplyAcls import *


LOG_FILE_NAME = 'test-qwalk-log-file.txt'


def test_search(creds, args, search, snapshot=None):
    log_it("Search: %s" % search)
    w = QWalkWorker(creds, Search(search), args.d,
                    None, LOG_FILE_NAME, None)
    w.run(snapshot)
    if os.path.exists(LOG_FILE_NAME):
        content = re.sub(r'[\r\n]+', ' ', open(LOG_FILE_NAME).read())
        log_it("FOUND!  : Search found - %s" % content)
        os.remove(LOG_FILE_NAME)
    else:
        log_it("NOTFOUND: Search failure: %s" % search)


def main():
    parser = argparse.ArgumentParser(description='Test the qwalk.py script')
    parser.add_argument('-s', help='Qumulo hostname', required=True)
    parser.add_argument('-u', help='Qumulo API user', 
                              default=os.getenv('QUSER') or 'admin')
    parser.add_argument('-p', help='Qumulo API password',
                              default=os.getenv('QPASS') or 'admin')
    parser.add_argument('-d', help='Test Directory', default='/')

    try:
        args, other_args = parser.parse_known_args()
    except:
        print("-"*80)
        parser.print_help()
        print("-"*80)
        sys.exit(0)

    # Everything will happen in a new subdirectory.
    test_dir_name = 'test-qwalk'
    creds = {"QHOST": args.s, "QUSER": args.u, "QPASS": args.p}
    log_it("Log in to: %s" % (args.s))
    rc = RestClient(creds["QHOST"], 8000)
    rc.login(creds["QUSER"], creds["QPASS"])
    parent_dir = '/'
    if args.d != '/':
        parent_dir = re.sub(r'/$', '', args.d)
    log_it("Create directory: %s/%s" % (parent_dir if parent_dir != '/' else '', test_dir_name))
    test_dir = rc.fs.create_directory(dir_path = parent_dir, name=test_dir_name)
    args.d = "%s/%s" % (parent_dir, 'test-qwalk')
    flowers_dir = rc.fs.create_directory(dir_path = args.d, name='flowers')
    foods_dir = rc.fs.create_directory(dir_path = args.d, name='foods')

    log_it("Create files")
    f = {}
    f["cat"] = rc.fs.create_file(dir_id=test_dir['id'], name='cat.txt')
    f["mouse"] = rc.fs.create_file(dir_id=test_dir['id'], name='mouse.jpg')
    f["dog"] = rc.fs.create_file(dir_id=test_dir['id'], name='dog.jpeg')
    f["bear"] = rc.fs.create_file(dir_id=test_dir['id'], name='bear.mov')
    f["rose"] = rc.fs.create_file(dir_id=flowers_dir['id'], name='rose.jpg')
    f["violet"] = rc.fs.create_file(dir_id=flowers_dir['id'], name='violet.jpg')
    f["cherry"] = rc.fs.create_file(dir_id=flowers_dir['id'], name='cherry.mpeg')
    f["pasta"] = rc.fs.create_file(dir_id=foods_dir['id'], name='pasta.txt')
    f["greenbeans"] = rc.fs.create_file(dir_id=foods_dir['id'], name='greenbeans.txt')
    f["rice"] = rc.fs.create_file(dir_id=foods_dir['id'], name='rice.txt')
    f["sushi"] = rc.fs.create_file(dir_id=foods_dir['id'], name='寿.漢')
    f["sushi_test"] = rc.fs.create_file(dir_id=foods_dir['id'], name='寿.test')
    rc.fs.set_file_attr(id_=f["greenbeans"]["id"], mode='0000')
    log_it("Write data to files")
    f_size = 1
    for k, v in f.items():
        fw = io.BytesIO(b'0123456789' * f_size)
        fw.seek(0)
        rc.fs.write_file(data_file = fw, id_ = v["id"])
        f_size *= 4
        fw.close()


    print("-" * 80)
    log_it("Test ApplyAcls")
    log_it("acls: %s" % len(rc.fs.get_acl(id_ = f["pasta"]['id'])['acl']['aces']))
    w = QWalkWorker(creds, 
                    ApplyAcls(["--replace_acls", "examples/acls-everyone-all-access.json"]), 
                    foods_dir['path'],
                    True, LOG_FILE_NAME, None)
    w.run()
    log_it("acls: %s" % len(rc.fs.get_acl(id_ = f["pasta"]['id'])['acl']['aces']))
    w = QWalkWorker(creds, 
                    ApplyAcls(["--add_entry", "examples/ace-everyone-read-only.json"]), 
                    foods_dir['path'],
                    True, LOG_FILE_NAME, None)
    w.run()
    log_it("acls: %s" % len(rc.fs.get_acl(id_ = f["pasta"]['id'])['acl']['aces']))
    log_it("Done Test ApplyAcls")
    print("-" * 80)

    log_it("Test snapshot search after deleting file")
    snap = rc.snapshot.create_snapshot(name="test-qwalk", id_=test_dir['id'])
    rc.fs.delete(id_=f['pasta']['id'])
    test_search(creds, args, ['--str', 'pasta'], snap["id"])
    rc.snapshot.delete_snapshot(snap['id'])
    log_it("Deleted test snapshot")
    print("-" * 80)

    log_it("Start: DataReductionTest")
    w = QWalkWorker(creds, DataReductionTest(['--perc', '1']), args.d,
                    True, LOG_FILE_NAME, None)
    w.run()
    print("." * 80)
    print(open(DataReductionTest.FILE_NAME).read().strip())
    print("." * 80)
    w.run_class.work_done(w)
    os.remove(DataReductionTest.FILE_NAME)
    log_it("Done with DataReductionTest")
    print("-" * 80)

    log_it("Start: ModeBitsChecker")
    w = QWalkWorker(creds, ModeBitsChecker, args.d,
                    True, LOG_FILE_NAME, None)
    w.run()
    print("." * 80)
    print(open(ModeBitsChecker.FILE_NAME).read().strip())
    print("." * 80)
    w.run_class.work_done(w)
    os.remove(ModeBitsChecker.FILE_NAME)
    log_it("Done with ModeBitsChecker")
    print("-" * 80)

    log_it("Start: SummarizeOwners")
    w = QWalkWorker(creds, SummarizeOwners, args.d,
                    True, LOG_FILE_NAME, None)
    w.run()
    w.run_class.work_done(w)
    print("-" * 80)

    test_search(creds, args, ['--re', '.*jpeg'])
    log_it("Start: ChangeExtension: 'jpeg' to 'jpg'")
    w = QWalkWorker(creds, ChangeExtension(['--from', 'jpeg', '--to', 'jpg']), args.d,
                    True, LOG_FILE_NAME, None)
    w.run()
    log_it("Done : ChangeExtension: 'jpeg' to 'jpg'")
    test_search(creds, args, ['--re', '.*jpeg'])
    print("-" * 80)
    test_search(creds, args, ['--re', '.*'])
    print("-" * 80)
    test_search(creds, args, ['--str', 'rose'])
    print("-" * 80)
    test_search(creds, args, ['--str', 'pig'])
    print("-" * 80)

    log_it("Delete directory: %s/%s" % (parent_dir if parent_dir != '/' else '', test_dir_name))
    rc.fs.delete_tree(id_ = test_dir['id'])


if __name__ == "__main__":
    main()