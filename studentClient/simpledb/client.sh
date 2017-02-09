# !/bin/bash

cd ../..
javac studentClient/simpledb/GroupBy.java
cd studentClient/simpledb
java -cp simpledb.jar:. TestGroupBy


#java -cp simpledb.jar:. SQLInterpreter

# test basic query
# select * from student

# test count
# select dname,count(dname) from student,dept where majorid=did group by dname

#  test sum
# select sum(majorid) from student

# test multiple group by
# select count(sid), majorid, gradyear from student group by majorid,gradyear

# test order by int, string
# select * from student,dept where majorid=did order by gradyear desc, dname
# select * from student order by sid desc
# select * from student order by gradyear, majorid desc limit 5

select sname, dname,gradyear from student, dept where majorid = did and gradyear=2004;