Yamr is a simple mapreduce framework which is mainly written in Erlang.

How to start:
1. start web server:
   python lib/fe.py
2. start Erlang distribution env:
   erl -rsh ssh -sname yamr -pa /home/bobos/yamr/ebin -setcookie YAMR -logs /home/bobos/tmp -connect_all false -nfs_storage /home/bobos/tmp/yamr

How to use:
1. create cluster:
    curl http://WebServerName:5000/cluster/ClusterName -H "Content-Type: application/json" -d '{"slave quota":"int", "timeout":"int"}' -X POST
2. add server to cluster: 
    curl http://WebServerName:5000/cluster/ClusterName/add -H "Content-Type: application/json" -d '{"timeout":"int","servers":{"hostname":"int", "hostname2":"int"}' -X PUT
3. submit job to cluster:
    curl http://WebServerName:5000/job -H "Content-Type: application/json" -d '{"timeout":"int","cluster":"ClusterName","callback module":"name","language":"type","code path":"path","version number":"int","priority":"int","partition":"int","job file":"pathToFileInJson"}' -X POST

Debug on slaves:
   create a cluster then start a slave node manually as:
   erl -rsh ssh -sname yamrslave_1_netest_cluster -setcookie YAMR -connect_all false -pa /home/bobos/yamr/ebin -logs /home/bobos/tmp -yamrmaster yamr@eselnts1107 -nfs_storage /home/bobos/tmp/yamr
   then run yamr_slave:start()

TODO: 
   1. replace flask+tornade as node.js for non-blocking i/o
   2. add more basic functions
   3. support python as callback
