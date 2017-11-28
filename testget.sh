yes | rm -r ./Cglogs/*

scp -r sparkmaster:~/utility/Cglog/* /Users/kotoda/sp_logbase/Cglogs
scp -r worker00:~/utility/Cglog/* /Users/kotoda/sp_logbase/Cglogs
scp -r worker01:~/utility/Cglog/* /Users/kotoda/sp_logbase/Cglogs


