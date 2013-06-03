-define(MASTER, yamrmaster).
-define(SLAVE, yamrslave).

-define(LOG(DebugInfo), ?LOG("~p", [DebugInfo])).
-define(LOG(Format, Args), spawn(fun()->write_to_log(Format, Args, ?LINE) end)).

-define(idle, idle).
-define(run, running).
-define(map, mapping).
-define(reduce, reducing).
-define(remove, to_be_removed).

-record(slave, {name::node(), 
                server::string(), 
                jobname::string(),
                state=?idle::?idle|?run|?map|?reduce|?remove}).

-record(server, {name::string(),
                 job_blacklist=[]::[{string(), integer()}]}).

-record(cluster, {name::string(), 
                  quota::integer(),
                  pincode::binary(), 
                  slaves=[]::[#slave{}],
                  servers=[]::[#server{}]}).
-record(job, {name::string(),
              clustername::string(),
              cb_module::string(),
              code_path::string(),
              language::string(),
              vsn::integer(),
              priority::integer(),
              partition::integer(),
              jobfile::string(),
              number_of_tasks=-1::integer(),
              progress_index=0::integer(),
              phase=map::map|reduce,
              client::pid()}).
-record(map_task, {clustername::string(),
                   jobname::string(),
                   partition::integer(),
                   priority::integer(),
                   task::{string(), binary()}}).
-record(running_map_task, {slave::node(), 
                           task::#map_task{}}).

-record(reduce_tasks, {jobname::string(),
                       running_tasks=[]::[{integer(), node()}],
                       queue_tasks=[]::[integer()]}).

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


