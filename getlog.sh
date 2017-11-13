scp sparkmaster:~/utility/EXElogs/$1 ~/sp_logbase/EXElogs
scp -r sparkmaster:/opt/spark/work/$1 ~/sp_logbase/GClogs
scp -r worker00:/opt/spark/work/$1 ~/sp_logbase/GClogs
scp -r worker01:/opt/spark/work/$1 ~/sp_logbase/GClogs
