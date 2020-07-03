import os
import re
import sys
import time
import argparse
from qwalk_worker import QWalkWorker


def each_file(file_obj, work_obj):
    # this is a custom method that replaces each file ending in ext_from with ext_to
    ext_from = ".jptg"
    ext_to   = ".jpeg"
    if file_obj['path'][-len(ext_from):] == ext_from:
        (dir_name, from_file_name) = os.path.split(file_obj['path'])
        to_file_name = from_file_name.replace(ext_from, ext_to)
        if work_obj.MAKE_CHANGES:
            work_obj.rc.fs.rename(name = to_file_name,
                                  source = file_obj['path'],
                                  dir_id = file_obj["dir_id"],
                                  )
        return "%s: %s -> %s" % (dir_name, from_file_name, to_file_name)

#################### the only thing you'll need to change is above ####################


def each_directory(file_list, work_obj):
    results = []
    for file_obj in file_list:
        result = each_file(file_obj, work_obj)
        if result:
            results.append(result)
    if len(results) > 0:
        with work_obj.result_file_lock:
            fw = open(work_obj.LOG_FILE_NAME, "a")
            for d in results:
                fw.write("%s\n" % d)
            fw.close()
            work_obj.action_count.value += len(results)


def main():
    parser = argparse.ArgumentParser(description='Walk Qumulo filesystem and do that thing.')
    parser.add_argument('-s', help='Qumulo hostname', required=True)
    parser.add_argument('-u', help='Qumulo API user', 
                              default=os.getenv('QUSER') or 'admin')
    parser.add_argument('-p', help='Qumulo API password',
                              default=os.getenv('QPASS') or 'admin')
    parser.add_argument('-d', help='Starting directory', required=True)
    parser.add_argument('-g', help='Run with filesystem changes', action='store_true')
    parser.add_argument('-l', help='Log file',
                              default='output-walk-log.txt')
    try:
        args = parser.parse_args()
    except:
        print("-"*60)
        parser.print_help()
        print("-"*60)
        sys.exit(0)

    w = QWalkWorker({"QHOST": args.s, "QUSER": args.u, "QPASS": args.p}, 
                    each_directory, 
                    50,      # number of client threads
                    args.d,   # starting directory
                    args.g,
                    args.l
                    )
    w.run()

if __name__ == "__main__":
    main()