# Qumulo filesystem walk with python and the API

Walk a Qumulo filesystem, perform actions with highly parallelized python

## Requirements

* MacOSX - python 2.7.16 + 3.7.7
* Linux  - python 2.7.15 + 3.7.6
* Qumulo API python bindings `pip install -r requirements.txt`

## How it works

This is a slightly complicated approach that is designed to handle billions of files and directories. Because billions of files and directories is a lot there are a number of optimizations added to this tool.

## Examples

`python qwalk.py -s product.eng.qumulo.com -d /gravytrain-tommy/hosting-backup`