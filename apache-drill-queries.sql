# most common extensions
select ext, count(1) FROM dfs.`/path/output*.parq` GROUP BY ext ORDER BY 2 DESC LIMIT 10;

# paths with ext "tommy"
select count(name) from dfs.`/path/*.parq` where regexp_matches(path, '.*tommy.*');

# paths beginning with /home/tommy
select count(name) from dfs.`/path/*.parq` where regexp_matches(path, '/home/tommy.*');

# paths with most items
select path, count(1) FROM dfs.`/path/*.parq` GROUP BY path ORDER BY 2 DESC LIMIT 10;
