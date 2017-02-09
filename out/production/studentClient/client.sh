# !/bin/bash

cd ../..
javac studentClient/simpledb/GroupBy.java
cd studentClient/simpledb
java -cp simpledb.jar:. TestGroupBy


#java -cp simpledb.jar:. SQLInterpreter

# test count
# select dname,count(dname) from student,dept where majorid=did group by dname

# test max
# select dname, max(gradyear) from student,dept where majorid=did group by dname

# test multiple group by
# select count(sid), majorid, gradyear from student group by majorid,gradyear

# test min
# select min(gradyear) from student

# test basic query
# select sname from student

# test sum
# select sum(majorid) from student
# select sum(majorid) from student group by gradyear

# test avg
# select avg(majorid) from student
# select avg(majorid) from student group by gradyear