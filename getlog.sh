scp sparkmaster:~/utility/EXElogs/$1 ~/sp_logbase/EXElogs
scp -r sparkmaster:/opt/spark/work/$1 ~/sp_logbase/GClogs
#scp -r worker00:/opt/spark/work/$1 ~/sp_logbase/GClogs
scp -r worker01:/opt/spark/work/$1 ~/sp_logbase/GClogs
mkdir /Users/kotoda/sp_logbase/Cglogs/$1
scp -r sparkmaster:~/utility/Cglog/* /Users/kotoda/sp_logbase/Cglogs/$1/
#scp -r worker00:~/utility/Cglog/* /Users/kotoda/sp_logbase/Cglogs/$1/
scp -r worker01:~/utility/Cglog/* /Users/kotoda/sp_logbase/Cglogs/$1/
