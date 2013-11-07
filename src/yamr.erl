-module(yamr).

-export([all2one/2,
         all2text/2,
         result2text/2,
         all2ets/2,
         result2ets/2,
         print_result/1]).

all2one(SrcDir, OutFile) ->
    AllData =
    lists:foldl(fun(File, Acc) -> 
                lists:append(Acc, read_result(File)) end,
                [], filelib:wildcard(filename:join(SrcDir, "*.result"))),
    file:write_file(OutFile, list_to_binary(io_lib:format("~p", [AllData])),
                    [append]).

all2text(SrcDir, DstDir) ->
    lists:foldl(fun(File, Fs) -> [result2text(File, DstDir)|Fs] end, 
                [], filelib:wildcard(filename:join(SrcDir, "*.result"))).

result2text(ResultFile, DstDir) ->
    FileName = filename:join([DstDir, filename:basename(ResultFile)]),
    {ok, Bin} = file:read_file(ResultFile),
    file:write_file(FileName, 
                    list_to_binary(io_lib:format("~p", [binary_to_term(Bin)]))),
    FileName.

all2ets(SrcDir, DstDir) ->
    lists:foldl(fun(File, Tabs) -> [result2ets(File, DstDir)|Tabs] end, 
                [], filelib:wildcard(filename:join(SrcDir, "*.result"))).

result2ets(ResultFile, DstDir) ->
    TabName = filename:basename(filename:rootname(ResultFile)),
    Tab = list_to_atom(TabName),
    Tab = ets:new(Tab, [named_table, set, public]),
    lists:foreach(fun({K, V}) -> true = ets:insert(Tab, {K, V}) end, 
                  read_result(ResultFile)),
    ok = ets:tab2file(Tab, filename:join([DstDir, TabName])),
    true = ets:delete(Tab),
    filename:join([DstDir, TabName]).

print_result(ResultFile) ->
    io:format("~p~n", [read_result(ResultFile)]).

%% return [{term(), term()}]
read_result(File) ->
    {ok, Bin} = file:read_file(File),
    binary_to_term(Bin).
