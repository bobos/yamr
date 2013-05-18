-define(MASTER, yamrmaster).
-define(SLAVE, yamrslave).
-define(CLUSTER_TAB, yamr_cluster_tab).
-define(RUNNING_TAB, yamr_running_job_tab).
-define(QUEUE_TAB, yamr_job_queue_tab).

-define(LOG(DebugInfo), ?LOG("~p", [DebugInfo])).
-define(LOG(Format, Args), spawn(fun()->write_to_log(Format, Args, ?LINE) end)).

-define(idle, idle).
-define(run, running).
-define(remove, to_be_removed).

-record(slave, {name::node(), 
                server::string(), 
                state=?idle::?idle|?run|?remove}).
-record(cluster, {name::string(), 
                  quota::integer(),
                  pincode::binary(), 
                  slaves=[]::[#slave{}]}).

-export([write_to_log/3]).
write_to_log(Format, Args, Line) ->
    {ok,[[LogPath]]} = init:get_argument(logs),
    LogFile = lists:append([LogPath, "/", atom_to_list(node()), ".log"]),
    {ok, Fd} = file:open(LogFile, [append]),
    %% timestamp
    {_Date,{Hour,Minute,Second}} = erlang:localtime(),
    {_MegaSec,_Sec, Micro} = erlang:now(),
    TimeFormat = "~2.10.0B:~2.10.0B:~2.10.0B.~6.10.0B ",
    io:fwrite(Fd,TimeFormat++"~p:~p:"++Format++"~n",
              [Hour,Minute,Second,Micro] ++ [?MODULE, Line] ++ Args),
    file:close(Fd).


