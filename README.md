# Qumulo filesystem walk with python and the API

Walk a Qumulo filesystem, perform actions with highly parallelized python

## Requirements

* MacOSX - python 2.7.16 + 3.7.7
* Linux  - python 2.7.15 + 3.7.6
* Qumulo API python bindings `pip install -r requirements.txt`


## How it works

This is approach is designed to handle billions of files and directories. Because billions of files and directories is a lot there are a number of optimizations added to this tool, including:

* Ability to run on only a specified subdirectory
* Leverage all Qumulo cluster nodes for extra power
* Multiprocessing queue to leverage Qumulo's scale and performance
* Local on-disk queue for when the in-process queue grows too large
* Progress updates every 10 seconds to confirm it's working
* Handle API bearer token timeout after 10 hours
* Break down large directories into smaller chunks
* Batch up small sets of files and directories when possible


## How fast it is?

It can read over 150,000 files per second and up to 6,000 directories per second. Generally, the script is more bound by number of directories than number of files. If there are things happening with each file that you add into the `each_file` method, you will very likely end up limited by the client cpu.


## Examples

`python qwalk.py -s product.eng.qumulo.com -d / -c SummarizeOwners`

This example runs walks the filesystem and summarizes owners and their corresponding file count and capacity utilization.

`python qwalk.py -s product.eng.qumulo.com -d / -c ChangeExtension`

This example runs walks the filesystem and looks for a file extension of your specification. If it finds that extension, it will log the potential rename operation. If you add the `-g` flag, it will rename the file.


## Working with "each_file"

`{
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
`