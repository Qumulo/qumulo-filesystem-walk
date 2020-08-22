import pandas
import datetime
import time
import sys
import os
import re
import glob
import math
import ciso8601
import multiprocessing



def process_lines(file_name, file_id, rows_per_file):
   print("Start %s." % (file_id))
   st = time.time()
   df = pandas.read_csv(file_name, sep='|', 
            names=['dir_id', 'id', 'f_type', 'name', 'size', 'blocks', 'owner', 'ts'], 
            dtype={'dir_id': 'int64', 'id': 'int64', 'f_type':'category', 
                   'name': 'str', 'size':'int64', 
                   'blocks': 'int32', 'owner':'category', 'ts':'str'},
            parse_dates=['ts'],
            date_parser=lambda x: ciso8601.parse_datetime(x[:10]).date(),
            skiprows=rows_per_file*file_id,
            nrows=rows_per_file)
   df['name'] = df['name'].str.lower()
   print("Created %s in %s seconds." % (file_id, int(time.time() - st)))
   df.to_parquet("%s-%s.parq" % (file_name, file_id))


def csv_to_parq():
   fname = 'output-walk-log.txt'
   line_num = 0
   line_length = 0
   with open(fname) as f:
      for line in f:
         line_num += 1
         line_length += len(line)
         if line_num >= 1000:
            break

   rows_per_file = 10000000
   file_size = os.path.getsize(fname)
   avg_line_len = line_length / line_num
   files_needed = math.ceil((file_size / avg_line_len) / rows_per_file)

   pool = multiprocessing.Pool(16)
   res = []
   file_id = 1
   for file_id in range(0, files_needed+3):
      # process_lines(fname, file_id, rows_per_file)
      res.append(pool.apply_async(process_lines, (fname, file_id, rows_per_file)))
   pool.close()
   pool.join()

def search_file(file, s):
   df = pandas.read_parquet(file)
   found = df[df['name'].str.contains(s, na=False, flags=re.IGNORECASE, regex=True)]
   return found

def search_parq(s):
   pool = multiprocessing.Pool(16)
   res = []
   file_id = 1
   for f_name in glob.glob("output-walk-log.txt-*"):
      res.append(pool.apply_async(search_file, (f_name, s)))
   pool.close()
   pool.join()
   for r in res:
      print(r.get())

csv_to_parq()
# search_parq("tommy")
