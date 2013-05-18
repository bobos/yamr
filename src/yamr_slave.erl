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
    gen_server:start_link({local, ?SLAVE}, ?MODULE, [], []).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------
init([]) ->
    {ok,[[Master]]} = init:get_argument(yamrmaster),
    MasterName = list_to_atom(Master),
    {?MASTER, MasterName} ! {slave_up, node()},
    ?LOG("slave is up"),
    {ClusterName, _N, _Server} = split_slavename(node()),
    State = #state{clustername = ClusterName,
                   mastername = MasterName},
    {ok, start_heartbeat_daemon(State)}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, Pid, Info}, State) ->
    if Pid == State#state.heartbeat_daemon ->
            ?LOG("Hearbeat daemon is screwed:~p, restart it\n", [Info]),
            {noreply, start_heartbeat_daemon(State)};
       true -> {noreply, State} end;

handle_info({stop, ?MASTER}, _State) ->
    ?LOG("stop slave"),
    halt();

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
    {?MASTER, Master} ! {heart_beat, ClusterName, node(), self()},
    receive ack -> ok
    after 10*1000 ->
        ?LOG("Master unreachable, self-terminate"), 
        halt()
    end,
    start_heartbeat_daemon(ClusterName, Master).
