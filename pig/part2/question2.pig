users = LOAD 'users.txt' USING PigStorage('|') AS (userId:int,age:int,gender:chararray,occupation:chararray,zipCode:chararray);
lawyerMaleUsers = FILTER users BY gender=='M' AND occupation=='lawyer';
lawyerMaleUsers = ORDER lawyerMaleUsers BY age DESC;
oldestUser = LIMIT lawyerMaleUsers 1;
oldestUserId = FOREACH oldestUser GENERATE userId;
dump oldestUserId; --10