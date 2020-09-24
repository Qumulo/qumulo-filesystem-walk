# Qumulo filesystem walk with python and the API

Walk a Qumulo filesystem, perform actions with highly parallelized python


## Requirements

* MacOSX - python 2.7, 3.7 (Tested on 2.7.16 and 3.7.7)
* Linux  - python 2.7, 3.7 (Tested on 2.7.15 and 3.7.6)
* Windows - python 2.7, 3.7 (Tested on 2.7.15 and 3.7.8)
* Qumulo API python bindings `pip install -r requirements.txt`
* Qumulo cluster software version >= 2.13.0 (though some features might work on older versions)


## How it works

This is approach is designed to handle billions of files and directories. Because billions of files and directories is a lot there are a number of optimizations added to this tool, including:

* Plugin a variety of "classes" to support different actions
* Ability to run on only a specified subdirectory
* Leverage all Qumulo cluster nodes for extra power
* Multiprocessing queue to leverage Qumulo's scale and performance
* Local on-disk queue for when the in-process queue grows too large
* Progress updates every 10 seconds to confirm it's working
* Handle API bearer token timeout after 10 hours
* Break down large directories into smaller chunks
* Batch up small sets of files and directories when possible
* Works both with Python2 and Python3


## How fast it is?

It can read over 150,000 files per second and up to 6,000 directories per second. Generally, the script is more bound by number of directories than number of files. If there are things happening with each file that you add into the `each_file` method, you will very likely end up limited by the client cpu and you won't be able to achieve 150,000 files per second or 6,000 directories per second.


## Output and logging

By default, the walk will output information to the command line every 10 seconds that indicates 
the progress of the walk. The fields are abbreviated and correspond to the following:

* **dir** - directories traversed
* **inod** - files+directories+other stuff traversed
* **actn** - "action" events, like setting a permission
* **dir/s** - directories traversed per second in the last 10 second window
* **fil/s** - files traversed per second in the last 10 second window
* **q** - length of the queue (aka number of directories that need to be processed still)

By default, a log file will also be written of everything that you're searching, traversing, or action taken. That file will be named: **output-walk-log.txt**.


## What can I do with the qwalk.py tool?


### Summarize owners of filesystem capacity

`python qwalk.py -s product.eng.qumulo.com -d / -c SummarizeOwners`

This example walks the filesystem and summarizes owners and their corresponding file count and capacity utilization.


### Change the file extension names for certain files

`python qwalk.py -s product.eng.qumulo.com -d / -c ChangeExtension --from jpeg --to jpg`

This example walks the filesystem, searches for files ending with ".jpeg" and then logs what files would be changed. If you want to make the changes, run the script with the added `-g` argument. 


### Search filesystem paths and names by regular expression or string

`python qwalk.py -s product.eng.qumulo.com -d / -c Search --str password`

Search for files with the exact string 'password' in the path or name. Look for the output in `output-walk-log.txt` in the same directory.

`python qwalk.py -s product.eng.qumulo.com -d / -c Search --re ".*passw[or]*d.*"`

Case-insensitive search for files with the string 'password' or 'passwd' in the path or name.
Look for the output in `output-walk-log.txt` in the same directory.


### List everything (files, directories, etc) in the filesystem

`python qwalk.py -s product.eng.qumulo.com -d / -c Search --re "." --cols path,type,id,size,blocks,owner,change_time`

This "search" is basically looking for anything and everything. `--re "."` means look for any charcter in the path. With each results it will then print a single line to the output file that includes the specified `--cols`. If no cols are specified, just the path is saved to the output file. All columns will saved in pipe-delimited format "|".

All potential columns include:

* path - full path
* name - name of the item
* dir_id - integer id of the parent direcory
* type - the type of item, usually FS_FILE_TYPE_FILE or FS_FILE_TYPE_DIRECTORY
* id - integer id
* file_number - integer id
* change_time - last change timestamp
* creation_time - creation timestamp
* modification_time - last modified timestamp
* child_count - direct children if a directory
* num_links - links to this item. includes itself, so starts at 1
* size - size of the contents if a file
* datablocks - data block(4096 byte) count of the item
* metablocks - metadata block(4096 byte) count of the item
* blocks - total block(4096 byte) count of the item
* owner - owner integer id
* owner_details - details about the owner
* group - group integer id
* group_details details about the group
* mode - POSIX mode bits
* symlink_target_type - symbolic link target type

### Find all symbolic links (symlinks) in a path

`python qwalk.py -s product.eng.qumulo.com -d /test -c Search --itemtype link --cols path,type,id,size,blocks,owner,change_time`

This command will walk the filesystem and search for items that are symlinks. It will also list out the corresponding metadata specified by --cols


### Examine contents of files to check for data reduction potential

`python qwalk.py -s product.eng.qumulo.com -d / -c DataReductionTest --perc 0.01`

Walk the filesystem and open a random 1% of files (--perc 0.01) and use zlib.compress to verify how compressible the data in the file is. This class will only attempt to compress, at most, 12288 bytes in each file. Because each examined requires multiple operations, this can be slower than the other current walk classes.


### POSIX mode bits where the owner has no rights to the file or directory.

`python qwalk.py -s product.eng.qumulo.com -d / -c ModeBitsChecker`

This will look at the metadata on each file and write any results to a file where the file or directory looks like '0\*\*' on the mode bits.


### Add a new read ACE "access control entry" to all items in a directory

`python qwalk.py -s product.eng.qumulo.com -d /test -c ApplyAcls --add_entry examples/ace-everyone-read-only.json`

This will look at all items within the specified start path `-d` and then add a new ACE. Specifically, it will add the ace in the example file examples/ace-everyone-read-only.json. By default, it will only output the list of items that will be changed to a log file. If you want to apply the changes specified, please add the `-g` argument.


### Add a new 'traverse/execute' ACE "access control entry" to all (and only) subdirectories in a directory

`python qwalk.py -s product.eng.qumulo.com -d /test -c ApplyAcls --add_entry examples/ace-everyone-execute-traverse.json --dirs_only`

This will look at all items within the specified start path `-d` and then add a new execute/traverse ACE for the Authenticated Users SID as specified in examples/ace-everyone-execute-traverse.json. By default, it will only output the list of directories that will be changed to a log file. If you want to apply the changes specified, please add the `-g` argument.


### Replace *ALL* ACLs on all items in a directory

`python qwalk.py -s product.eng.qumulo.com -d /test -c ApplyAcls --replace_acls examples/acls-everyone-all-access.json`

This will look at all items within the specified start path `-d` and then replace the existing ACLs with the new ACls in the example file examples/acls-everyone-all-access.json. By default, it will only output the list of directories that will be changed to a log file. If you want to apply the changes specified, please add the `-g` argument.


### Copy a full directory tree

`python qwalk.py -s product.eng.qumulo.com -d /test -c CopyDirectory --to_dir /test-full-copy`

This will copy all items within the specified start directory `-d` to the destination directory `--to_dir`.


### Restore all data from a snapshot for the given directory.

`python qwalk.py -s product.eng.qumulo.com -d /test --snap 55123 -c CopyDirectory --to_dir /test-full-copy-from-snap`

This will copy all items within the specified start directory `-d` and within the specified snapshot to the destination directory `--to_dir`.


## Parameters, knobs, tweaks, mostly for working on Windows

* **QBATCHSIZE** - the batch size of files and directories processed by the qtask jobs (default: 100 win, 400 othter)
* **QWORKERS** - the number of python worker processes in the worker pool (default: 30 win, 60 other)
* **QWAITSECONDS** - how often to wait between updates (default: 10 seconds)
* **QMAXLEN** - max queue length for the workers (default: 100,000 win, 300,000 other)
* **QUSEPICKLE** - the most expiremental of the knobs. Use pickled _files_ to pass batches around (default: false)

Set any of these variables at the command line:
* Windows: `Set QBATCHSIZE=100`
* Max/Linux: `export QBATCHSIZE=1000`


## Building qtask classes

Any walk of the filesystem will involve handling lots of files and directories. It also can involve a lot of different functionality and code. The qtask classes are where this functionality can be built. Above we have a number of classes currently built, but for those that know a bit of code, they can create their own classes or modify existing classes to meet their functional needs.

See the current imlementations in qtasks/ to figure out how to build your own approach.

For a bit of context that can help, below you will find the metadata that we have with each file inside of the `every_batch` method.

```{
 'dir_id': '5160036463',
 'type': 'FS_FILE_TYPE_FILE'
 'id': '5158036745',
 'file_number': '5158036745',
 'path': '/gravytrain-tommy/hosting-backup/map-tile/vet02123002133313322.jpg',
 'name': 'vet02123002133313322.jpg',
 'change_time': '2018-03-31T22:04:48.877148926Z',
 'creation_time': '2018-03-31T22:04:48.870469026Z',
 'modification_time': '2015-11-25T07:15:51Z',
 'child_count': 0,
 'num_links': 1,
 'datablocks': '1',
 'blocks': '2',
 'metablocks': '1',
 'size': '3240',
 'owner': '12884901921',
 'owner_details': {'id_type': 'NFS_UID', 'id_value': '33'},
 'group': '17179869217',
 'group_details': {'id_type': 'NFS_GID', 'id_value': '33'},
 'mode': '0644',
 'symlink_target_type': 'FS_FILE_TYPE_UNKNOWN',
 'extended_attributes': {'archive': True,
                         'compressed': False,
                         'hidden': False,
                         'not_content_indexed': False,
                         'read_only': False,
                         'sparse_file': False,
                         'system': False,
                         'temporary': False},
 'directory_entry_hash_policy': None,
 'major_minor_numbers': {'major': 0, 'minor': 0},
}
```

Additional data can be extracted per file, such as acls, alternate data streams, and other details. That additional data will require additional API calls, and will slow down the walk.

