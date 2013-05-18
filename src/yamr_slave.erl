-module(yamr_slave).
-behaviour(gen_server).

-export([start/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-import(yamr_master, [split_slavename/1]).

-record(state, {heartbeat_daemon::pid(),
                clustername::string(),
                mastername::node()}).

-include("yamr.hrl").

start() ->
    {ClusterName, SlaveName, _N, _Server} = split_slavename(node()),
    gen_server:start_link({local, SlaveName}, ?MODULE, 
                          [#state{clustername=ClusterName}], []).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------
init([State]) ->
    {ok,[[Master]]} = init:get_argument(yamrmaster),
    MasterName = list_to_atom(Master),
    try ok = gen_server:call({?MASTER, MasterName}, {slave_up, node()})
    catch Err:Reason -> ?LOG("slave failed ~p:~p", [Err, Reason]), halt() end,
    ?LOG("slave is up"),
    {ok, start_heartbeat_daemon(State#state{mastername = MasterName})}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast({stop, ?MASTER}, _State) ->
    ?LOG("stop slave"),
    halt();

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, Pid, Info}, State) ->
    if Pid == State#state.heartbeat_daemon ->
            ?LOG("hearbeat daemon is screwed:~p, restart it\n", [Info]),
            {noreply, start_heartbeat_daemon(State)};
       true -> {noreply, State} end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_heartbeat_daemon(State) when is_record(State, state) ->
    Pid = spawn(fun() -> start_heartbeat_daemon(State#state.clustername, 
                                                State#state.mastername) end),
    erlang:monitor(process, Pid),
    State#state{heartbeat_daemon = Pid}.
start_heartbeat_daemon(ClusterName, Master) when is_list(ClusterName) ->
    %% send heartbeat to master every 20s
    timer:sleep(20*1000),
    try ack = gen_server:call({?MASTER, Master},
                              {heart_beat, ClusterName, node()}, 10*1000)
    catch Err:Reason -> ?LOG("master unreachable due to ~p:~p, "
                             "self-terminating", [Err, Reason]), halt() end,
    start_heartbeat_daemon(ClusterName, Master).
