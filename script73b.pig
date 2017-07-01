REGISTER /home/acadgild/hadoop/hdfs/piggybank-0.15.0.jar

DEFINE ApacheCombinedLogLoader org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader();

DEFINE DayExtractor org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor('yyyy-MM-dd');

weblogs = LOAD '/user/acadgild/hadoop/sample_log' USING ApacheCombinedLogLoader AS(ipaddress, logname, user, timestamp, method, location, protocol, returnobject, bytes, pagelink, broswer, OS);

days= GROUP weblogs by DayExtractor(timestamp);

days_hits = FOREACH days GENERATE group , COUNT (weblogs) as hits;

sorted_hits = ORDER days_hits by hits desc;

dump sorted_hits;
