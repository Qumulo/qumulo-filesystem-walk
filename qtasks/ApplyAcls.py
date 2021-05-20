import os
import io
import argparse
import json
import qumulo.commands.fs as fs
import qumulo.rest.fs as rest_fs
from argparse import Namespace


class ApplyAcls:
    def __init__(self, args=None):
        parser = argparse.ArgumentParser(description="")
        parser.add_argument("--replace_acls", help="")
        parser.add_argument("--add_entry", help="")
        parser.add_argument("--dirs_only", action="store_true", help="")
        args = parser.parse_args(args)
        self.replace_acls = None
        self.add_entry = None
        self.dirs_only = False
        if args.dirs_only:
            self.dirs_only = True
        if args.replace_acls:
            self.replace_acls = json.loads(open(args.replace_acls).read())
        elif args.add_entry:
            new_entry = json.loads(open(args.add_entry).read())
            self.add_entry = Namespace(
                id=None,
                path=None,
                rights=new_entry["rights"] if "rights" in new_entry else None,
                type=new_entry["type"] if "type" in new_entry else None,
                flags=new_entry["flags"] if "flags" in new_entry else None,
                trustee=new_entry["trustee"] if "trustee" in new_entry else None,
                insert_after=None,
                json=True,
            )

    @staticmethod
    def every_batch(file_list, work_obj):
        results = []
        action_count = 0
        for file_obj in file_list:
            if (
                work_obj.run_class.dirs_only
                and file_obj["type"] != "FS_FILE_TYPE_DIRECTORY"
            ):
                continue
            action_count += 1
            try:
                status = "nochange"
                if work_obj.MAKE_CHANGES:
                    if work_obj.run_class.add_entry:
                        work_obj.run_class.add_entry.id = file_obj["id"]
                        fs.do_add_entry(
                            rest_fs,
                            work_obj.rc.conninfo,
                            work_obj.rc.credentials,
                            work_obj.run_class.add_entry,
                        )
                        status = "*changed*"
                    elif work_obj.run_class.replace_acls:
                        work_obj.rc.fs.set_acl_v2(
                            id_=file_obj["id"], acl=work_obj.run_class.replace_acls
                        )
            except Exception as e:
                status = "**failed**"
                print(e)
            results.append("%s|%s|%s" % (status, file_obj["id"], file_obj["path"]))
            if action_count >= 100:
                with work_obj.result_file_lock:
                    work_obj.action_count.value += action_count
                action_count = 0

        if len(results) > 0:
            with work_obj.result_file_lock:
                fw = io.open(work_obj.LOG_FILE_NAME, "a", encoding="utf8")
                for d in results:
                    fw.write("%s\n" % d)
                fw.close()
                work_obj.action_count.value += action_count

    @staticmethod
    def work_start(work_obj):
        if os.path.exists(work_obj.LOG_FILE_NAME):
            os.remove(work_obj.LOG_FILE_NAME)

    @staticmethod
    def work_done(work_obj):
        pass
