-module(yamr_slave).
-behaviour(gen_server).

-export([start/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-import(yamr_master, [split_slavename/1]).

-include("yamr.hrl").
-record(state, {state=?idle::?idle|?map|?reduce,
                heartbeat_daemon::{pid(), reference()},
                map_worker::{pid(), reference()},
                reduce_worker::{pid(), reference()},
                jobname=[]::string(),
                clustername::string(),
                mastername::node()}).

-define(TMP_DB, tmp_db).

start() ->
    {ClusterName, SlaveName, _N, _Server} = split_slavename(node()),
    ?LOG("slave process Name ~p", [SlaveName]),
    {ok, Pid} = gen_server:start_link({local, SlaveName}, ?MODULE, 
                                      [#state{clustername=ClusterName}],
                                      [{spawn_opt, [{fullsweep_after, 1000}]}]),
    erlang:monitor(process, Pid),
    monitor(Pid).

monitor(Pid) ->
    timer:sleep(1000),
    receive 
        {'DOWN', _, process, Pid, Info} -> ?LOG("slave dies:~p", [Info]), halt()
    end,
    monitor(Pid).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------
init([State]) ->
    {ok,[[Master]]} = init:get_argument(yamrmaster),
    MasterName = list_to_atom(Master),
    try ok = call_master(slave_up, MasterName, 5000)
    catch Err:Reason -> ?LOG("slave failed ~p:~p", [Err, Reason]), halt() end,
    ?LOG("slave is up"),
    {ok, start_heartbeat_daemon(State#state{mastername = MasterName})}.

handle_call({dump_remain_map_data, Job}, _From, State) 
            when State#state.jobname == Job#job.name ->
    ?LOG("dump all map data from tabs for ~p", [Job#job.name]),
    dump_remain_map_data(Job),
    {reply, ack, State};

handle_call({map_report, ReduceIdxs, JobName}, _From, State) ->
    cast_master({map_report, ReduceIdxs, JobName}, State#state.mastername),
    case State#state.map_worker of
        {_, Ref} ->
            erlang:demonitor(Ref);
        _ -> ok
    end,
    {reply, ok, State};

handle_call({reduce_report, ReduceIdx, JobName}, _From, State) ->
    ?LOG("reduce report from worker for ~p", [ReduceIdx]),
    cast_master({reduce_report, ReduceIdx, JobName}, State#state.mastername),
    case State#state.reduce_worker of
        {_, Ref} ->
            erlang:demonitor(Ref);
        _ -> ok
    end,
    {reply, ok, State};

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast({stop, Reason, ?MASTER}, State) ->
    ?LOG("stop slave reason:~p", [Reason]),
    if Reason =:= normal ->
        cast_master(normal_stop, State#state.mastername);
       true -> ok end,
    halt();

handle_cast({map, Job, Task}, State) ->
    NewState = handle_map_task(Job, Task, State),
    {noreply, NewState};

handle_cast({reduce, Job, Task}, State) ->
    ?LOG("handle reduce task for ~p", [Task]),
    NewState = handle_reduce_task(Job, Task, State),
    {noreply, NewState};

handle_cast({map_done, JobName}, State) when JobName == State#state.jobname ->
    ?LOG("final reduce phase for ~p", [JobName]),
    case State#state.reduce_worker of
        {Pid, _Ref} ->
            Pid ! map_done;
        _ -> ok
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, Pid, Info}, State) ->
    Information = {Pid, MonitorRef}, 
    if Information == State#state.heartbeat_daemon ->
           ?LOG("hearbeat daemon is screwed:~p, restart it", [Info]),
           {noreply, start_heartbeat_daemon(State)};
       Information == State#state.map_worker ->
           ?LOG("map worker dies:~p", [State#state.jobname]),
           %%FIXME
           %%cast_master(map_task_crashes, State#state.mastername);
           halt();
       Information == State#state.reduce_worker ->
           ?LOG("reduce worker dies:~p", [State#state.jobname]),
           %%FIXME
           halt();
           %%cast_master(reduce_task_crashes, State#state.mastername);
       true -> {noreply, State} end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_a_worker(Fun) when is_function(Fun) ->
    Pid = spawn(Fun),
    Ref = erlang:monitor(process, Pid),
    {Pid, Ref}.

start_heartbeat_daemon(State) when is_record(State, state) ->
    Fun = fun() -> start_heartbeat_daemon(State#state.clustername, 
                                          State#state.mastername) end,
    State#state{heartbeat_daemon = start_a_worker(Fun)}.
start_heartbeat_daemon(ClusterName, Master) when is_list(ClusterName) ->
    %% send heartbeat to master every 20s
    timer:sleep(20*1000),
    try ack = call_master({heart_beat, ClusterName}, Master, 10*1000)
    catch Err:Reason -> ?LOG("master unreachable due to ~p:~p, "
                             "self-terminating", [Err, Reason]), halt() end,
    start_heartbeat_daemon(ClusterName, Master).

handle_map_task(Job, Task, State) when Job#job.language =:= "Erlang" ->
    NewState=
    if State#state.jobname =:= Job#job.name ->
        State;
       true -> 
        prepare_env(Job, ?map),
        State#state{jobname=Job#job.name}
    end,
    Parent = self(),
    Fun = fun() -> map(Job, Task, Parent) end,
    NewState#state{state = ?map, map_worker = start_a_worker(Fun)}.

handle_reduce_task(Job, Task, State) when Job#job.language =:= "Erlang" ->
    JobName = Job#job.name,
    case {State#state.jobname, State#state.state} of
        {JobName, SlaveStat} ->
            if SlaveStat == ?map -> dump_remain_map_data(Job);
               SlaveStat == ?idle -> prepare_env(Job, ?reduce);
               true -> ok end,
            NewState = State#state{state = ?reduce},
            Parent = self(),
            Fun = fun() -> reduce(Job, Task, Parent, 
                                  Job#job.phase == reduce) end,
            NewState#state{reduce_worker = start_a_worker(Fun)};
        _ ->
            State
    end.

dump_remain_map_data(Job) ->
    ?LOG("dump all remaining map data for ~p", [Job#job.name]),
    Pid = self(),
    spawn(fun() -> yamr_file:dump_tabs(Job), Pid ! all_dumped end),
    receive
        all_dumped -> yamr_file:delete_tabs()
    after 60000 -> 
        ?LOG("dump timeout for ~p", [Job#job.name]), 
        halt() 
    end.

map(Job, Task, Pid) ->
    CBModule = list_to_atom(Job#job.cb_module),
    {Key, ValueBin} = Task#map_task.task,
    KVs = CBModule:map(Key, binary_to_list(ValueBin)),
    Ret = pre_reduce(CBModule, aggre_values(KVs, lists, false), 
                     Task#map_task.partition, []),
    Idxs = [Idx||{Idx, KVs1}<-Ret, 
            begin yamr_file:write(Job, Idx, KVs1), true end],
    gen_server:call(Pid, {map_report, Idxs, Job#job.name}).

reduce(Job, Idx, Pid, NoLoop) ->
    init_db(),
    CB = list_to_atom(Job#job.cb_module),
    Fun = fun() ->
            retreive_map_data(Job, Idx),
            dump_db(Job, Idx, CB),
            gen_server:call(Pid, {reduce_report, Idx, Job#job.name}),
            exit(self(), exit)
          end,
    if NoLoop -> Fun();
       true -> loop(Job, Idx, CB, Pid, Fun) end.

loop(Job, Idx, CB, Pid, Fun) ->
    receive 
        map_done ->
            Fun()
    after 100 -> ok
    end,
    retreive_map_data(Job, Idx),
    loop(Job, Idx, CB, Pid, Fun).

retreive_map_data(Job, Idx) ->
    KVs = yamr_file:read(Job, Idx),
    %lists:foldl(
    %    fun({K, V}, Acc) ->
    %        NewV = CB:reduce(K, V),
    %        [{K, NewV}|Acc]
    %    end, [], aggre_values(yamr_file:read(Job, Idx), lists)),
    write_db(KVs).

prepare_env(Job, Phase) ->
    if Phase =:= ?map -> yamr_file:init(Job); true -> ok end,
    true = code:add_pathz(Job#job.code_path),
    ?LOG("~p environment for ~p is ready", [Phase, Job#job.name]).

pre_reduce(_, [], _, Acc) ->
    Acc;
pre_reduce(CB, [{K,Vs}|Rest], Range, Acc) ->
    NewValue = CB:reduce(K, Vs),
    Idx = erlang:phash2(K, Range),
    NewAcc =
    case lists:keyfind(Idx, 1, Acc) of
        false -> [{Idx, [{K, NewValue}]}|Acc];
        {Idx, KVs} -> lists:keyreplace(Idx, 1, Acc, {Idx, [{K, NewValue}|KVs]})
    end,
    pre_reduce(CB, Rest, Range, NewAcc).

cast_master(Msg, Master) ->
    gen_server:cast({?MASTER, Master}, {Msg, node()}).
call_master(Msg, Master, Timeout) ->
    gen_server:call({?MASTER, Master}, {Msg, node()}, Timeout).

init_db() ->    
    catch ets:delete(?TMP_DB),
    ?TMP_DB = ets:new(?TMP_DB, [named_table, bag]).

write_db([]) ->
    ok;
write_db([{Key, Value}|Rest]) ->
    true = ets:insert(?TMP_DB, {Key, term_to_binary(Value)}),
    write_db(Rest).

dump_db(Job, Idx, CB) ->
    KVs = 
    lists:foldl(
        fun({K, V}, Acc) ->
            NewV = CB:reduce(K, V),
            [{K, NewV}|Acc]
        end, [], aggre_values(?TMP_DB, ets, true)),
    yamr_file:result(Job, Idx, KVs).
                  
aggre_values(Input, Method, Compressed)->
    Method:foldl(
        fun({K, V}, Acc) ->
            NewV = if Compressed -> binary_to_term(V); true -> V end,
            case lists:keyfind(K, 1, Acc) of
                false -> [{K, [NewV]}|Acc];
                {K, Vs} -> lists:keyreplace(K, 1, Acc, {K, [NewV|Vs]})
            end end, [], Input).
