-module(tcp_daemon).

-export([start_link/0]).

-export([start_server/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("yamr.hrl").

-define(TCP_PORT, 4567).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%----------------------------------------------------------------------------
%%% gen_server callbacks
%%%----------------------------------------------------------------------------
init([]) ->
    Pid = spawn_link(?MODULE, start_server, []),
    {ok, Pid}.

handle_call(_Msg, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_server() ->
    {ok,IP} = inet:getaddr(net_adm:localhost(),inet),
    case gen_tcp:listen(get_port(), [list, {active, false},{ip, IP}]) of
        {ok, LSock} ->
            accept_connections(LSock);
        _ ->
            ok
    end.

accept_connections(LSock) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            spawn(fun() -> do_recv(Sock) end),
            accept_connections(LSock);
        {error, _Reason} ->
            %% something wrong with listen socket, close it
            gen_tcp:close(LSock)
    end.

do_recv(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Msg} ->
            Reply = gen_server:call({global, ?MASTER}, Msg),
            gen_tcp:send(Sock, Reply);
        _ ->
            ok
    end.

get_port() ->
    case init:get_argument(tcp_port) of
            error -> ?TCP_PORT;
            {ok,[[Port]]} -> list_to_integer(Port)
    end.
