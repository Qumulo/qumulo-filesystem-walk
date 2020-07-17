import re
import os
import io
import argparse
import qumulo.commands.fs as fs
import qumulo.rest.fs as rest_fs
from argparse import Namespace

class ApplyAcls:

    @staticmethod
    def add_inherited_execute_and_read_for_authed_users(file_id, rc):
        ns = Namespace(id = file_id,
                        path = None,
                        rights = ['EXECUTE', 'READ_ACL'],
                        type = 'ALLOWED',
                        flags = ['CONTAINER_INHERIT', 'INHERITED'],
                        trustee = 'sid:S-1-5-11',
                        insert_after = None,
                        json = True,
                    )
        fs.do_add_entry(rest_fs, rc.conninfo, rc.credentials, ns)

    @staticmethod
    def every_batch(file_list, work_obj):
        results = []
        for file_obj in file_list:
            if file_obj['type'] == 'FS_FILE_TYPE_DIRECTORY':
                try:
                    status = "nochange"
                    if work_obj.MAKE_CHANGES:
                        ApplyAcls.add_inherited_execute_and_read_for_authed_users(file_obj['id'], work_obj.rc)
                        status = "*changed*"
                except Exception as e:
                    status = "**failed**"
                    print(e)
                results.append("%s|%s|%s" % (status, file_obj['id'], file_obj['path']))

        if len(results) > 0:
            with work_obj.result_file_lock:
                fw = io.open(work_obj.LOG_FILE_NAME, "a", encoding='utf8')
                for d in results:
                    fw.write("%s\n" % d)
                fw.close()
                work_obj.action_count.value += len(results)

    @staticmethod
    def work_start(work_obj):
        if os.path.exists(work_obj.LOG_FILE_NAME):
            os.remove(work_obj.LOG_FILE_NAME)

    @staticmethod
    def work_done(work_obj):
        pass