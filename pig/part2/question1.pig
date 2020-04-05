users = LOAD 'users.txt' USING PigStorage('|') AS (userId:int,age:int,gender:chararray,occupation:chararray,zipCode:chararray);
grouped = GROUP users BY (gender,occupation);
groupedCount = FOREACH grouped GENERATE FLATTEN(group), COUNT(users) AS count;
maleLawyer = FILTER groupedCount BY gender=='M' AND occupation=='lawyer';
result = FOREACH maleLawyer GENERATE $2;
dump result; --10