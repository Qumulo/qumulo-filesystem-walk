import pandas
import datetime
import time
import sys
import os
import re
import glob
import random
import math
import ciso8601
import multiprocessing



def process_lines(file_name, file_id, rows_per_file):
   print("Start %s." % (file_id))
   st = time.time()
   df = pandas.read_csv(file_name, sep='|', 
            names=['dir_id', 'id', 'f_type', 'path', 'name', 'size', 'blocks', 'owner', 'ts', 'link_target'], 
            dtype={'dir_id': 'int64', 'id': 'int64', 'f_type':'category', 
                   'path': 'str', 'name': 'str', 'size':'int64', 
                   'blocks': 'int32', 'owner':'category', 'ts':'str', 'link_target':'str'},
            parse_dates=['ts'],
            date_parser=lambda x: ciso8601.parse_datetime(x[:10]).date(),
            skiprows=rows_per_file*file_id,
            nrows=rows_per_file)
   df['name'] = df['name'].str.lower()
   print("Created %s in %s seconds." % (file_id, int(time.time() - st)))
   df.to_parquet("%s-%s.parq" % (file_name, file_id))


def csv_to_parq(fname):
   line_num = 0
   line_length = 0
   with open(fname) as f:
      for line in f:
         if random.random() < 0.005:
            line_num += 1
            line_length += len(line)
            if line_num >= 100000:
               break

   rows_per_file = 6000000
   file_size = os.path.getsize(fname)
   avg_line_len = line_length / line_num
   files_needed = math.ceil((file_size / avg_line_len) / rows_per_file)

   pool = multiprocessing.Pool(16)
   res = []
   file_id = 1
   for file_id in range(0, files_needed):
      # process_lines(fname, file_id, rows_per_file)
      res.append(pool.apply_async(process_lines, (fname, file_id, rows_per_file)))
   pool.close()
   pool.join()

def search_file(file, s):
   df = pandas.read_parquet(file)
   found = df[df['name'].str.contains(s, na=False, flags=re.IGNORECASE, regex=True)]
   return {"results": found, "searched_count": len(df)}

def search_parq(file, s):
   pool = multiprocessing.Pool(16)
   res = []
   file_id = 1
   for f_name in glob.glob("%s-*.parq" % file):
      res.append(pool.apply_async(search_file, (f_name, s)))
   pool.close()
   pool.join()
   for r in res:
      d = r.get()
      for index, row in d["results"].iterrows():
         print("%(id)12s - %(f_type)17s - %(path)s" % row)

if sys.argv[1] == "import_initial":
   csv_to_parq(sys.argv[2])
elif sys.argv[1] == "search":
   search_parq(sys.argv[2], sys.argv[3])
