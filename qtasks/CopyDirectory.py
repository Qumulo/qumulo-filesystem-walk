import re
import os
import io
import sys
import argparse

class CopyDirectory:
    def __init__(self, args):
        parser = argparse.ArgumentParser(description='')
        parser.add_argument('--to_dir', help='')
        args = parser.parse_args(args)
        self.to_dir = None
        self.cols = ['path']
        if args.to_dir:
            self.to_dir = args.to_dir
        self.folders = {}

    def create_folder(self, rc, path):
        if path in self.folders:
            return self.folders[path]
        levels = path.split("/")
        for level in range(2, len(levels)+1):
            new_dir = '/'.join(levels[0:level])
            if new_dir not in self.folders:
                try:
                    new_f = rc.fs.get_file_attr(path = new_dir)
                    self.folders[new_dir] = new_f['id']
                    continue
                except:
                    print("get_file_attr exception: %s %s" % (sys.exc_info()[0], new_dir))
                    pass
                dir_path = '/'.join(levels[0:level-1])
                if dir_path == '':
                    dir_path = '/'
                dir_name = levels[level-1]
                try:
                    new_f = rc.fs.create_directory(dir_path=dir_path, name=dir_name)
                except:
                    print("create_directory exception: %s %s/%s" % (sys.exc_info()[0], dir_path, dir_name))
                    new_f = rc.fs.get_file_attr(path = new_dir)
                    self.folders[new_dir] = new_f['id']

                self.folders[new_dir] = new_f['id']
        return self.folders[path]

    @staticmethod
    def every_batch(file_list, work_obj):
        results = []
        for file_obj in file_list:
            to_path = file_obj['path'].replace(work_obj.start_path, work_obj.run_class.to_dir)
            parent_path = os.path.dirname(to_path)
            file_name = os.path.basename(to_path)
            work_obj.run_class.create_folder(work_obj.rc, parent_path)
            if file_obj['type'] == "FS_FILE_TYPE_DIRECTORY":
                work_obj.run_class.create_folder(work_obj.rc, to_path)
                results.append("DIRECTORY  : %s -> %s" % (file_obj['path'], to_path))
            else:
                try:
                    new_f = work_obj.rc.fs.get_file_attr(path = to_path)
                    if new_f['size'] == file_obj['size'] and \
                            new_f['modification_time'] == file_obj['modification_time']:
                        results.append("FILE EXISTS: %s -> %s" % (file_obj['path'], to_path))
                        continue
                    else:
                        work_obj.rc.fs.delete(id_ = new_f['id'])
                except:
                    # print("File copy exception: %s %s" % (sys.exc_info()[0], file_obj['path']))
                    pass
                try:
                    work_obj.rc.fs.create_file(dir_path=parent_path, name=file_name)
                    work_obj.rc.fs.copy(source_id = file_obj["id"],
                                        target_path = to_path, 
                                        source_snapshot = work_obj.snap)
                    results.append("FILE COPIED: %s -> %s" % (file_obj['path'], to_path))
                except:
                    # print("File copy exception: %s %s" % (sys.exc_info()[0], file_obj['path']))
                    results.append("!!FILE COPY FAILED: %s -> %s" % (file_obj['path'], to_path))

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