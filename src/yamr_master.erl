-module(yamr_master).
-behaviour(gen_server).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([split_slavename/1]).
-record(state, {}).
-include("yamr.hrl").

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MASTER}, ?MODULE, [], []).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------
init([]) ->
    ?CLUSTER_TAB = ets:new(?CLUSTER_TAB, [named_table, set, public]),
    ?RUNNING_TAB = ets:new(?RUNNING_TAB, [named_table, set, public]),
    ?QUEUE_TAB = ets:new(?QUEUE_TAB, [named_table, set, public]),
    {ok, #state{}}.

handle_call({create_cluster, {ClusterName, SlaveQuota}}, _From, State) ->
    {reply, create_cluster(ClusterName, SlaveQuota), State};

handle_call({add_server, {ClusterName,ServerName,NoOfSlaves}}, From, State) ->
    %% spawn a worker to start slave, since start slave takes time it will
    %% block master badly if let master do it
    spawn(fun()-> start_slaves(From, ClusterName, ServerName, NoOfSlaves) end),
    {noreply, State};

handle_call({remove_server, {ClusterName, ServerName, NoOfSlaves}}, 
            _From, State) ->
    remove_slaves(ClusterName, ServerName, list_to_integer(NoOfSlaves)),
    {reply, "OK", State};

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({slave_up, Node}, State) ->
    ?LOG("slave ~p up", [Node]),
    monitor_node(Node, true),
    add_slave(Node),
    {noreply, State};
       
%% heartbeat from slaves
handle_info({heart_beat, ClusterName, Node, From}, State) ->
    case get_cluster(ClusterName) of
        not_found -> ok;
        Cluster ->
            case [S||S<-Cluster#cluster.slaves, S#slave.name == Node] of
                [] -> ok;
                _ -> From ! ack
            end
    end,
    {noreply, State};

handle_info({nodedown, Node}, State) ->
    ?LOG("Slave down:~p", [Node]),
    restart_slave_if_needed(Node),
    {noreply, State};

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
    case start_one_slave(ClusterName, ServerName, Args) of
        {ok, Node} ->
            lists:append([Reply, " ", atom_to_list(Node), ":ok"]);
        {error, Node, Reason} ->
            lists:append([Reply, " ", atom_to_list(Node), ":", 
                          atom_to_list(Reason)]);
        {error, Reason} ->
            lists:append([Reply, " None:", atom_to_list(Reason)])
    end,
    start_slaves(ClusterName, ServerName, NoOfSlaves-1, Args, NewReply).

start_one_slave(ClusterName, Server) ->
    Args = erl_sys_args(),
    start_one_slave(ClusterName, Server, Args).

start_one_slave(ClusterName, Server, Args) ->
    case get_a_slavename(ClusterName, Server) of
        {ok, Slave} ->
            ?LOG("Start slave ~p on ~s with command ~p", 
                 [Slave, Server, Args]),
            case slave:start(Server, Slave, Args) of
                {ok, Node} ->
                    {ok, Node};
                Reason ->
                    ?LOG("Failed to start slave:~p", [Reason]),
                    {error, Slave, failed_to_start_slave}
            end;
        Error ->
            Error
    end.

get_a_slavename(ClusterName, Server) ->
    case get_cluster(ClusterName) of
        not_found ->
            {error, cluster_not_found};
        Cluster ->
            Quota = Cluster#cluster.quota,
            List = [list_to_integer(No)||S<-Cluster#cluster.slaves, 
                    begin {_,No,_} = split_slavename(S#slave.name),
                          S#slave.server == Server end],
            if Quota =< length(List) ->
                  {error, quota_reached};
               true ->
                  NewNo = if [] == List -> 1;
                             true -> lists:last(lists:sort(List))+1 end,
                  {ok, get_slavename(ClusterName, NewNo)}
            end
    end.

add_slave(NodeName) ->
    {ClusterName, _No, Server} = split_slavename(NodeName),
    case get_cluster(ClusterName) of
        not_found ->
            ?LOG("WARNING!! Not possible to get here, cluster:~p", 
                 [ClusterName]),
            ok;
        Cluster ->
            Slave = #slave{name=NodeName, server=Server},
            Slaves = [Slave|Cluster#cluster.slaves],
            update_cluster(Cluster#cluster{slaves=Slaves})
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
                                     S#slave.state == ?run -> {I, [S|R]};
                                     S#slave.state == ?remove -> {I, R}
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
                                     begin stop_slave(S#slave.name), true end])
    end.

stop_slave(SlaveName) ->
    {?SLAVE, SlaveName} ! {stop, ?MASTER}.

restart_slave_if_needed(Node) ->
    {ClusterName, _No, Server} = split_slavename(Node),
    NoRestart =
    case get_cluster(ClusterName) of
        not_found ->
            true;
        Cluster ->
            NewSlaves = [S||S<-Cluster#cluster.slaves, S#slave.name =/= Node],
            update_cluster(Cluster#cluster{slaves=NewSlaves}),
            case Cluster#cluster.slaves -- NewSlaves of
                [] -> true;
                %% FIXME: put task back to waiting queue
                [S] when S#slave.state == ?run -> false;
                [S] when S#slave.state == ?idle -> false;
                [S] when S#slave.state == ?remove -> true
            end
    end,
    NoRestart orelse spawn(fun() -> start_one_slave(ClusterName, Server) end).

-spec erl_sys_args() -> string().
erl_sys_args() ->
  {ok,[[LogPath]]} = init:get_argument(logs),
  {ok,[[ErlPath]]} = init:get_argument(pa),
  lists:append(["-rsh ssh -noshell -setcookie ",
                atom_to_list(erlang:get_cookie()), " -connect_all false",
                " -s yamr_slave start -logs ", LogPath, " -pa ", ErlPath,
                " -yamrmaster ", atom_to_list(node())]).

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

%% yamr_slave-No-ClusterName@Server
get_slavename(ClusterName, SlaveNo) when is_integer(SlaveNo) ->
    list_to_atom(lists:append([atom_to_list(?SLAVE), "_", 
                               integer_to_list(SlaveNo), "_", ClusterName])).

split_slavename(Node) when is_atom(Node) ->
    [SlavePrefix, Server] = string:tokens(atom_to_list(Node), "@"),
    [Slave, No|_] = string:tokens(SlavePrefix, "_"),
    ClusterName = SlavePrefix -- lists:append([Slave, "_", No, "_"]),
    {ClusterName, list_to_integer(No), Server}.

