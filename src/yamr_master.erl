-module(yamr_master).
-behaviour(gen_server).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([split_slavename/1]).
-record(state, {switch_phase_slaves=[]::[{node(), string()}],
                normal_stop_slaves=sets:new()}).

-include("yamr.hrl").

-define(CLUSTER_TAB, yamr_cluster_tab).
-define(MAP_TAB, yamr_map_task_tab).
-define(REDUCE_TAB, yamr_reduce_task_tab).

-define(INIT_TASK_Num, 500).
-define(SLAVE_RETRIES, 3).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MASTER}, ?MODULE, [], 
                          [{spawn_opt, [{fullsweep_after, 100}]}]).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------
init([]) ->
    ?CLUSTER_TAB = ets:new(?CLUSTER_TAB, [named_table, set, public]),
    ?MAP_TAB = ets:new(?MAP_TAB, [named_table, set, public]),
    ?REDUCE_TAB = ets:new(?REDUCE_TAB, [named_table, set, public]),
    yamr_job:init(),
    {ok, #state{}}.

handle_call({create_cluster, [ClusterName, SlaveQuota]}, _From, State) ->
    {reply, create_cluster(ClusterName, SlaveQuota), State};

handle_call({add_server, [ClusterName,ServerName,NoOfSlaves]}, From, State) ->
    %% spawn a worker to start slave, since start slave takes time it will
    %% block master badly if let master do it
    case get_cluster(ClusterName) of
        not_found ->
            reply_client(From, "Cluster "++ClusterName++" isn't found");
        Cluster ->
            Quota = Cluster#cluster.quota,
            case Quota < list_to_integer(NoOfSlaves) of
               true ->
                reply_client(From, "Cluster "++ClusterName++" doesn't allow "
                                   ++NoOfSlaves++" slaves on one server");
               false ->
                 spawn(fun()-> start_slaves(From, ClusterName, ServerName,
                                            NoOfSlaves) end)
            end
    end,
    {noreply, State};

handle_call({remove_server, [ClusterName, ServerName, NoOfSlaves]}, 
            _From, State) ->
    remove_slaves(ClusterName, ServerName, list_to_integer(NoOfSlaves)),
    {reply, "OK", State};

handle_call({slave_up, Node}, _From, State) ->
    ?LOG("slave ~p up", [Node]),
    monitor_node(Node, true),
    add_slave(Node),
    NewState =
    case lists:keyfind(Node, 1, State#state.switch_phase_slaves) of
        false ->
            find_a_task4slave(Node),
            State;
        {Node, JobName} ->
            %%continue to take reduce tasks
            case get_reduce_tasks(JobName) of
                [] ->
                    gen_server:cast(?MASTER, {slave_idle, Node});
                _ ->
                    take_one_reduce_task(JobName, Node)
            end,
            State#state{switch_phase_slaves=
                    lists:keydelete(Node, 1, State#state.switch_phase_slaves)}
    end,
    {reply, ok, NewState};
       
%% heartbeat from slaves
handle_call({{heart_beat, ClusterName}, Node}, _From, State) ->
    case get_cluster(ClusterName) of
        not_found -> {noreply, State};
        Cluster -> case [S||S<-Cluster#cluster.slaves, S#slave.name == Node] of
                        [] -> {noreply, State}; _ -> {reply, ack, State} end
    end;

handle_call({submit_job, JobInfo}, From, State) ->
    ?LOG("job received: ~p",[JobInfo]),
    [ClusterName|_] = JobInfo,
    case get_cluster(ClusterName) of
        not_found ->
            {reply, "Cluster doesn't exist", State};
        Cluster ->
            handle_job(JobInfo, Cluster, From),
            {noreply, State#state{}}
    end;

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast({{map_report, ReduceIdxs, JobName}, SlaveName}, State) ->
    remove_map_task(JobName, SlaveName),
    store_reduce_tasks(ReduceIdxs, JobName),
    find_a_task4slave(JobName, SlaveName) == ok orelse map_done(JobName),
    {noreply, State};

handle_cast({{reduce_report, ReduceIdx, JobName}, SlaveName}, State) ->
    continue_reduce_tasks(ReduceIdx, JobName, SlaveName),
    {noreply, State};

handle_cast({slave_idle, Node}, State) ->
    find_a_task4slave(Node),
    {noreply, State};

handle_cast({normal_stop, Node}, State) ->
    {noreply, 
     State#state{normal_stop_slaves=
                 sets:add_element(Node, State#state.normal_stop_slaves)}};

handle_cast({{switch_phase, JobName}, Node}, State) ->
    {noreply, 
     State#state{switch_phase_slaves=
                 lists:keystore(Node, 1, State#state.switch_phase_slaves, 
                                {Node, JobName})}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodedown, Node}, State) ->
    ?LOG("slave down:~p", [Node]),
    remove_possible_lock(Node),
    NoBlackList = 
    sets:is_element(Node, State#state.normal_stop_slaves) orelse
    begin case lists:keyfind(Node, 1, State#state.switch_phase_slaves) of
                false -> false; _ -> true end end,
    restart_slave_if_needed(Node, NoBlackList),
    {noreply, 
     State#state{normal_stop_slaves=
                 sets:del_element(Node, State#state.normal_stop_slaves)}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%---------------------------------------------------------------------------
%%% internal functions
%%%---------------------------------------------------------------------------

%%%---------------------------------------------------------------------------
    -spec create_cluster(Name::string(), Quota::string()) -> string().
%%%
%%% create a yamr cluster
%%%---------------------------------------------------------------------------
create_cluster(Name, Quota) ->
    case get_cluster(Name) of
       not_found ->
           update_cluster(#cluster{name=Name, quota=list_to_integer(Quota)}),
           "ok";
       _ ->
           "already exist"
    end.

start_slaves(From, ClusterName, ServerName, NoOfSlaves) ->
    Args = erl_sys_args(),
    Reply = start_slaves(ClusterName, ServerName, list_to_integer(NoOfSlaves), 
                         Args, []),
    gen_server:reply(From, Reply).

start_slaves(_, _, 0, _, Reply) ->
    Reply;
start_slaves(ClusterName, ServerName, NoOfSlaves, Args, Reply) ->
    NewReply =
    case get_a_slavename(ClusterName, ServerName, NoOfSlaves) of
        {ok, Slave} ->
            case start_one_slave(ServerName, Slave, Args) of
                {ok, Node} ->
                    lists:append([Reply, " ", atom_to_list(Node), ":ok"]);
                {error, Node, Reason} ->
                    lists:append([Reply, " ", atom_to_list(Node), ":", 
                                  atom_to_list(Reason)])
            end;
        {error, Reason} ->
            lists:append([Reply, " None:", atom_to_list(Reason)])
    end,
    start_slaves(ClusterName, ServerName, NoOfSlaves-1, Args, NewReply).

start_one_slave(Slave) ->
    [SlaveName, Server] = string:tokens(atom_to_list(Slave), "@"),
    Args = erl_sys_args(),
    start_one_slave(Server, list_to_atom(SlaveName), Args).

start_one_slave(Server, Slave, Args) ->
    ?LOG("start slave ~p on ~s with command ~p", [Slave, Server, Args]),
    Fun = fun(0, _F, R) ->
               ?LOG("failed to start slave:~p", [R]),
               {error, Slave, failed_to_start_slave};
             (Retry, F, _) ->
               case slave:start(Server, Slave, Args) of
                   {ok, Node} -> {ok, Node};
                   Reason -> F(Retry-1, F, Reason)
               end
          end,
    Fun(?SLAVE_RETRIES, Fun, ok).

get_a_slavename(ClusterName, Server, No) ->
    case get_cluster(ClusterName) of
        not_found ->
            {error, cluster_not_found};
        Cluster ->
            Quota = Cluster#cluster.quota,
            List = [N||S<-Cluster#cluster.slaves, 
                    begin {_,_,N,_} = split_slavename(S#slave.name),
                          S#slave.server == Server end],
            if Quota =< length(List) ->
                  {error, quota_reached};
               true ->
                  NewNo = if [] == List -> No;
                             true -> lists:last(lists:sort(List))+No end,
                  {ok, get_slavename(ClusterName, NewNo)}
            end
    end.

add_slave(NodeName) ->
    {ClusterName, _, _No, Server} = split_slavename(NodeName),
    case get_cluster(ClusterName) of
        not_found ->
            ?LOG("WARNING!! Not possible to get here, cluster:~p", 
                 [ClusterName]),
            ok;
        Cluster ->
            Servers =
            case [Svr||Svr<-Cluster#cluster.servers, 
                  Svr#server.name =:= Server] of
                [] ->
                    [#server{name=Server}|Cluster#cluster.servers];
                _ ->
                    Cluster#cluster.servers
            end,
            Slave = #slave{name=NodeName, server=Server},
            Slaves = [Slave|Cluster#cluster.slaves],
            update_cluster(Cluster#cluster{slaves=Slaves, servers=Servers})
    end.

remove_slaves(ClusterName, Server, NoOfSlaves) ->
    case get_cluster(ClusterName) of
        not_found ->
            ok;
        Cluster ->
            Find = [S||S<-Cluster#cluster.slaves, S#slave.server =:= Server],
            SL =
            if NoOfSlaves >= length(Find) ->
                  %% remove all slaves
                  Find;
               true -> 
                  {Idles, Runnings} =
                  lists:foldl(fun(S, {I, R})-> 
                                  if S#slave.state == ?idle -> {[S|I], R};
                                     S#slave.state == ?remove -> {I, R};
                                     true -> {I, [S|R]}
                                  end end, {[], []}, Find),
                  if NoOfSlaves =< length(Idles) -> 
                         {L, _} = lists:split(NoOfSlaves, Idles), L;
                     true ->
                         {L, _} = 
                         lists:split(NoOfSlaves - length(Idles), Runnings),
                         Idles ++ L
                  end
            end,
            update_cluster(Cluster, [S#slave{state=?remove}||S<-SL, 
                                     begin stop_slave(S#slave.name, remove), 
                                           true end])
    end.

stop_slave(SlaveName, Reason) ->
    send2slave(SlaveName, {stop, Reason, ?MASTER}).

send2slave(SlaveName, Msg) ->
    {_, ProcessName, _, _} = split_slavename(SlaveName),
    gen_server:cast({ProcessName, SlaveName}, Msg).

restart_slave_if_needed(Node, NoBlackList) ->
    NoRestart =
    case get_cluster(Node) of
        not_found ->
            true;
        Cluster ->
            NewSlaves = [S||S<-Cluster#cluster.slaves, S#slave.name =/= Node],
            update_cluster(Cluster#cluster{slaves = NewSlaves}),
            case Cluster#cluster.slaves -- NewSlaves of
                [] -> true;
                [S] when S#slave.state == ?idle -> false;
                [S] when S#slave.state == ?remove -> true;
                %% FIXME: check reduce task queue if this is last running reduce
                [S] -> 
                    JobName = S#slave.jobname,
                    NoBlackList orelse update_job_blacklist(Node, JobName),
                    Job = yamr_job:get_job(JobName),
                    if S#slave.state == ?map ->
                         rollback_map_task(Node, Job);
                       true ->
                         rollback_reduce_task(Node, Job)
                    end,
                    false
            end
    end,
    NoRestart orelse spawn(fun() -> start_one_slave(Node) end).

-spec erl_sys_args() -> string().
erl_sys_args() ->
  {ok,[[LogPath]]} = init:get_argument(logs),
  {ok,[[ErlPath]]} = init:get_argument(pa),
  {ok,[[Storage]]} = init:get_argument(nfs_storage),
  lists:append(["-rsh ssh -noshell -setcookie ",
                atom_to_list(erlang:get_cookie()), " -connect_all false",
                " -s yamr_slave start -logs ", LogPath, " -pa ", ErlPath,
                " -yamrmaster ", atom_to_list(node()), " -nfs_storage ",
                Storage]).

get_cluster(SlaveName) when is_atom(SlaveName)->
    {ClusterName, _, _, _} = split_slavename(SlaveName),
    get_cluster(ClusterName);
get_cluster(ClusterName) ->
    case ets:lookup(?CLUSTER_TAB, ClusterName) of
        [] -> not_found;
        [{ClusterName, Cluster}] -> Cluster
    end.

update_cluster(Cluster) ->
    %% just let whole server crash if basic ets:insert doesn't work
    true = ets:insert(?CLUSTER_TAB, {Cluster#cluster.name, Cluster}).

update_cluster(Cluster, []) ->
    update_cluster(Cluster);
update_cluster(Cluster, [Slave|Rest]) ->
    update_cluster(update_cluster(Cluster, Slave), Rest);
update_cluster(Cluster, Slave) when is_record(Slave, slave) ->
    Slaves = [NewS||S<-Cluster#cluster.slaves, 
              begin NewS = 
                    if S#slave.name =:= Slave#slave.name -> Slave;
                       true -> S end, true end],
    Cluster#cluster{slaves=Slaves}.

%% yamr_slave_No_ClusterName@Server
get_slavename(ClusterName, SlaveNo) when is_integer(SlaveNo) ->
    list_to_atom(lists:append([atom_to_list(?SLAVE), "_", 
                               integer_to_list(SlaveNo), "_", ClusterName])).

split_slavename(Node) when is_atom(Node) ->
    [ProcessName, Server] = string:tokens(atom_to_list(Node), "@"),
    [Slave, No|_] = string:tokens(ProcessName, "_"),
    ClusterName = ProcessName -- lists:append([Slave, "_", No, "_"]),
    {ClusterName, list_to_atom(ProcessName), list_to_integer(No), Server}.

handle_job(JobInfo, Cluster, From) ->
    Slaves = get_slaves_by_state(Cluster, ?idle),
    case yamr_job:put_job(JobInfo, From, length(Slaves)*?INIT_TASK_Num) of
        ok ->  
            F = fun([], _Fun) -> ok;
                   ([S|Rest], Fun) -> 
                       gen_server:cast(?MASTER, {slave_idle, S#slave.name}),
                       Fun(Rest, Fun)
                end,
            length(Slaves) =:= 0 orelse F(Slaves, F);
        {error, Reason} ->
            reply_client(From, Reason)
    end.

map_task2slave(Task, SlaveName) ->
    JobName = Task#map_task.jobname,
    MapMsg = {map, yamr_job:get_job(Task#map_task.jobname), Task},
    RunningTask = #running_map_task{slave=SlaveName, task=Task},
    NewTasks =
    case ets:lookup(?MAP_TAB, JobName) of
        [] -> [RunningTask];
        [{_, Tasks}] -> [RunningTask|Tasks] end,
    true = ets:insert(?MAP_TAB, {JobName, NewTasks}),
    set_slave_state(SlaveName, JobName, ?map),
    send2slave(SlaveName, MapMsg).

get_slaves_by_state(Cluster, State) ->
    [S||S<-Cluster#cluster.slaves, S#slave.state =:= State].

store_reduce_tasks(Tasks, JobName) when is_record(Tasks, reduce_tasks) -> 
    true = ets:insert(?REDUCE_TAB, {JobName, Tasks});

store_reduce_tasks([], _JobName) -> ok;
store_reduce_tasks(ReduceIdxs, JobName) ->
    Tasks =
    case get_reduce_tasks(JobName) of
        [] ->
            #reduce_tasks{jobname=JobName, queue_tasks=ReduceIdxs};
        ReduceTasks ->
            Idxs = ReduceIdxs -- 
                   [Idx||{Idx,_}<-ReduceTasks#reduce_tasks.running_tasks],
            QTs = lists:usort(ReduceTasks#reduce_tasks.queue_tasks++Idxs),
            ReduceTasks#reduce_tasks{queue_tasks=QTs}
    end,
    store_reduce_tasks(Tasks, JobName).

get_reduce_tasks(JobName) ->
    case ets:lookup(?REDUCE_TAB, JobName) of
        [] -> [];
        [{_, ReduceTasks}] -> ReduceTasks
    end.

remove_reduce_tasks(JobName) ->
    true = ets:delete(?REDUCE_TAB, JobName).

continue_reduce_tasks(Idx, JobName, SlaveName) ->
    ReduceTasks = get_reduce_tasks(JobName),
    RestRuns = ReduceTasks#reduce_tasks.running_tasks -- [{Idx, SlaveName}],
    NewTasks = ReduceTasks#reduce_tasks{running_tasks = RestRuns}, 
    store_reduce_tasks(NewTasks, JobName),
    case ReduceTasks#reduce_tasks.queue_tasks of
        [] ->
            set_slave_state(SlaveName, ?idle),
            gen_server:cast(?MASTER, {slave_idle, SlaveName}),
            if RestRuns =:= [] ->
                  %% map reduce is done
                  %% FIXME
                  Job = yamr_job:get_job(JobName),
                  yamr_job:remove_job(JobName),
                  ResultFile = yamr_file:get_result(Job),
                  remove_reduce_tasks(JobName),
                  remove_map_tasks(JobName),
                  remove_from_blacklist(SlaveName, JobName),
                  reply_client(Job#job.client, "Results: "++ResultFile++"\n");
               true -> ok
            end;
        _ ->
            take_one_reduce_task(JobName, SlaveName)
    end.

find_a_task4slave(SlaveName) ->
    JobBlacklist = get_job_blacklist(SlaveName),
    case yamr_job:get_a_task_except(?INIT_TASK_Num, JobBlacklist) of
        [] -> JobName = any_reduce_task(JobBlacklist),
              JobName =:= [] orelse take_one_reduce_task(JobName, SlaveName);
        Task -> map_task2slave(Task, SlaveName)
    end.

any_reduce_task(JobBlacklist) ->
    JobNames = [J||{J, Tks}<-lists:reverse(ets:tab2list(?REDUCE_TAB)), 
                Tks#reduce_tasks.queue_tasks =/= []],
    case JobNames -- JobBlacklist of
       [] -> [];
       [JobName|_] -> JobName
    end.

find_a_task4slave(JobName, SlaveName) ->
    case yamr_job:get_a_task(JobName, ?INIT_TASK_Num) of
        [] -> 
            case get_reduce_tasks(JobName) of
                [] ->
                    %% tell slave to dump map data
                    {_, ProcessName, _, _} = split_slavename(SlaveName),
                    try ack = 
                        gen_server:call({ProcessName, SlaveName}, 
                                        {dump_remain_map_data, 
                                         yamr_job:get_job(JobName)}),
                        set_slave_state(SlaveName, ?idle),
                        gen_server:cast(?MASTER, {slave_idle, SlaveName})
                    catch _:_ -> stop_slave(SlaveName, force) end,
                    ok;
                _ ->
                    take_one_reduce_task(JobName, SlaveName),
                    [{_, Maps}] = ets:lookup(?MAP_TAB, JobName),
                    if Maps == [] -> map_done; true -> ok end
            end;
        Task ->
            map_task2slave(Task, SlaveName),
            ok
    end.

take_one_reduce_task(JobName, SlaveName) ->
    ReduceTasks = get_reduce_tasks(JobName),
    #reduce_tasks{running_tasks=Runs, queue_tasks=Waits} = ReduceTasks,
    case Waits =/= [] of
        true ->
            Job = yamr_job:get_job(JobName),
            [T|Rest] = Waits,
            send2slave(SlaveName, {reduce, Job, T}),
            set_slave_state(SlaveName, JobName, ?reduce),
            NewReduce = ReduceTasks#reduce_tasks{
                             running_tasks=[{T,SlaveName}|Runs],
                             queue_tasks=Rest},
            store_reduce_tasks(NewReduce, JobName);
        false -> 
            set_slave_state(SlaveName, ?idle),
            gen_server:cast(?MASTER, {slave_idle, SlaveName})
    end.

remove_map_task(JobName, Node) ->
    case ets:lookup(?MAP_TAB, JobName) of
        [] -> ok;
        [{_, Ts}] ->
            true = ets:insert(?MAP_TAB, 
                   {JobName, [T||T<-Ts, T#running_map_task.slave =/= Node]}) 
    end.

remove_map_tasks(JobName) ->
    true = ets:delete(?MAP_TAB, JobName).

map_done(JobName) ->
    ClusterName = yamr_job:get_clustername(JobName),
    Cluster = get_cluster(ClusterName),
    ?LOG("final reduce phase for ~p", [JobName]),
    Job = yamr_job:get_job(JobName),
    yamr_job:put_job(Job#job{phase=reduce}),
    lists:foreach(
        fun(S) when S#slave.jobname =:= JobName, S#slave.state =:= ?reduce ->
                send2slave(S#slave.name, {map_done, JobName});
           (_) -> ignore end, Cluster#cluster.slaves).

reply_client(Client, Reply) ->
    ?LOG("reply client:~p", [Reply]),
    gen_server:reply(Client, Reply).

set_slave_state(SlaveName, State) ->
    set_slave_state(SlaveName, undefined, State).

set_slave_state(SlaveName, JobName, State) ->
    Cluster = get_cluster(SlaveName),
    update_cluster(Cluster, [S#slave{state=State, jobname=JobName}
                             ||S<-Cluster#cluster.slaves, 
                             S#slave.name =:= SlaveName]).

rollback_map_task(Node, Job) ->
    case ets:lookup(?MAP_TAB, Job#job.name) of
        [] -> ok;
        [{_, Ts}] ->
            [Task] = [T#running_map_task.task||T<-Ts, 
                      T#running_map_task.slave =:= Node],
            true = ets:insert(?MAP_TAB, {Job#job.name, 
                              [T||T<-Ts, T#running_map_task.slave =/= Node]}), 
            yamr_job:put_task(Task),
            yamr_file:remove_map_files(Node, Job)
    end.

rollback_reduce_task(Node, Job) ->
    ReduceTasks = get_reduce_tasks(Job#job.name),
    [Idx] = [I||{I,S}<-ReduceTasks#reduce_tasks.running_tasks, S =:= Node],
    RestRuns = ReduceTasks#reduce_tasks.running_tasks -- [{Idx, Node}],
    NewTasks = ReduceTasks#reduce_tasks{running_tasks = RestRuns,
               queue_tasks=[Idx|ReduceTasks#reduce_tasks.queue_tasks]},
    yamr_file:remove_reduce_files(Node, Job, Idx),
    store_reduce_tasks(NewTasks, Job#job.name).

remove_possible_lock(Node) ->
    yamr_file:remove_possible_lock(atom_to_list(Node), get_jobname(Node)).

update_job_blacklist(Node, JName) ->
    Cluster = get_cluster(Node),
    {_, _, _, Server} = split_slavename(Node),
    [Svr] = [S||S<-Cluster#cluster.servers, S#server.name =:= Server],
    BL = Svr#server.job_blacklist,
    NewBL =
    case [NewRcd||Rcd<-BL, 
          begin {JobName, Cnt} = Rcd,
                NewRcd =
                  if JName == JobName ->
                       {JobName, Cnt+1};
                     true -> Rcd
                  end, true end] of
        BL ->
            [{JName, 1}|Svr#server.job_blacklist];
        NewList -> NewList
    end,
    Servers = lists:keyreplace(Server, 2, Cluster#cluster.servers, 
                               Svr#server{job_blacklist=NewBL}),
    update_cluster(Cluster#cluster{servers=Servers}). 

get_job_blacklist(Node) ->
    Cluster = get_cluster(Node),
    {_, _, _, Server} = split_slavename(Node),
    case [S||S<-Cluster#cluster.servers, S#server.name == Server] of
        [] -> [];
        [Svr] -> [JobName||{JobName, Cnt}<-Svr#server.job_blacklist, Cnt > 3]
    end.

get_jobname(Node) ->
    Cluster = get_cluster(Node),
    case [S||S<-Cluster#cluster.slaves, S#slave.name == Node] of
        [] -> [];
        [Slave] -> Slave#slave.jobname
    end.

remove_from_blacklist(Node, JobName) ->
    Cluster = get_cluster(Node),
    NewServers =
    [NewServer||S<-Cluster#cluster.servers, 
     begin 
         BL = [{J, C}||{J, C}<-S#server.job_blacklist, J =/= JobName],
         NewServer = S#server{job_blacklist = BL}, true end],
    update_cluster(Cluster#cluster{servers=NewServers}). 
