-module(yamr_file).

-export([init/1,
         delete_tabs/0,
         dump_tabs/1,
         write/3,
         read/2,
         result/3,
         get_result/1,
         remove_possible_lock/1,
         remove_map_files/2,
         remove_reduce_files/3]).

-include("yamr.hrl").

-define(TMP, "/home/eboosun/tmp/yamr/").
-define(INTERM, yamr_intermediate_tab_info).

init(Job) ->
    case ets:info(?INTERM) of
        undefined ->
            ok;
        _ ->
            delete_tabs()
    end,
    TabSize = get_max_tab_size(Job),
    ?INTERM = ets:new(?INTERM, [public, named_table, set]),
    Tabs = [Tab||Idx<-lists:seq(0, Job#job.partition-1),
            begin Tab = get_tab_name(Job, Idx),
                  Tab = ets:new(Tab, [public, named_table, set]), 
                  true = ets:insert(Tab, {index, Idx}), 
                  true = ets:insert(Tab, {size, 0}), 
                  true end],
    true = ets:insert(?INTERM, {maxsize, TabSize}),
    true = ets:insert(?INTERM, {tabs, Tabs}).

delete_tabs() ->
    [{tabs, Tabs}] = ets:lookup(?INTERM, tabs),
    lists:foreach(fun(Tab) -> catch ets:delete(Tab) end, Tabs),
    ets:delete(?INTERM).

dump_tabs(Job) ->
    [{tabs, Tabs}] = ets:lookup(?INTERM, tabs),
    lists:foreach(fun(Tab) -> write2file(Job, Tab) end, Tabs).

get_tab_name(Job, Idx) ->
    list_to_atom(filename:basename(get_prefix(Job, Idx))).

get_max_size() ->
    [{_, Size}] = ets:lookup(?INTERM, maxsize),
    Size.

get_size(Tab) ->
    [{_, Size}] = ets:lookup(Tab, size),
    Size.

get_index(Tab) ->
    [{_, Idx}] = ets:lookup(Tab, index),
    Idx.

write(Job, Idx, KVs) ->
    Tab = get_tab_name(Job, Idx),
    MxSiz = get_max_size(),
    CSiz = get_size(Tab),
    Bin = term_to_binary(KVs),
    IncrSiz = erlang:byte_size(Bin) + 3*8, %%3 words for size of tuple plus int
    NewSiz = IncrSiz+CSiz,
    true = ets:insert(Tab, {size, NewSiz}),
    true = ets:insert(Tab, {ets:info(Tab, size)+1, Bin}),
    MxSiz > NewSiz orelse write2file(Job, Tab).

remove_map_files(Node, Job) ->
    WC = lists:append([?TMP, Job#job.clustername, "_", Job#job.name, "_*_",
                       atom_to_list(Node), ".tmp"]),
    lists:foreach(fun(F) -> ?LOG("remove map file ~p", [F]), file:delete(F) end,
                  filelib:wildcard(WC)).

remove_reduce_files(Node, Job, Idx) ->
    FileName = get_prefix(Job, Idx) ++ atom_to_list(Node) ++ ".result",
    ?LOG("remove reduce file ~p", [FileName]),
    file:delete(FileName).

read(Job, Idx) ->
    read_file(get_files(Job, Idx), []).

result(Job, Idx, KVs) ->
    FileName = get_prefix(Job, Idx) ++ atom_to_list(node()) ++ ".result",
    {ok, Fd} = file:open(FileName, write),
    lists:foreach(fun({K, V}) -> io:format(Fd, "~p:~p~n", [K, V]) end, KVs),
    ok = file:close(Fd).

get_result(Job) ->
    Prefix = lists:append([?TMP, Job#job.clustername, "_", Job#job.name, "_"]),
    %% remove all tmp files
    lists:foreach(fun(F) -> file:delete(F) end, 
                  filelib:wildcard(Prefix++"*.tmp")),
    %% merge result files
    %ResultFiles = filelib:wildcard(Prefix++"*.result"),
    MegRes = lists:append([?TMP,Job#job.clustername,"_",Job#job.name,".all"]),
    %lists:foreach(fun(F) -> 
    %                {ok, Bin} = file:read_file(F),
    %                file:delete(F),
    %                file:write_file(MegRes, Bin, [append])
    %              end, ResultFiles),
    MegRes.

write2file(Job, Tab) ->
    Idx = get_index(Tab),
    FileName = get_filename(Job, Idx),
    Lock = get_lock(FileName),
    ets:tab2file(Tab, FileName),
    ets:delete_all_objects(Tab),
    ets:insert(Tab, {index, Idx}),
    ets:insert(Tab, {size, 0}),
    remove_lock(Lock).

read_file([], Acc) ->
    Acc;
read_file([File|Rest], Acc) ->
    Lock = get_lock(File),
    Accs =
    case filelib:is_file(File) of
        true ->
            {ok, Tab} = ets:file2tab(File),
            NewAcc =
            ets:foldl(fun({K, V}, Acc1) when is_integer(K) -> 
                             lists:append(Acc1, binary_to_term(V));
                         (_, Acc1) -> Acc1 end, Acc, Tab),
            ok = file:delete(File),
            true = ets:delete(Tab),
            remove_lock(Lock),
            NewAcc;
        false -> Acc 
    end,
    read_file(Rest, Accs).

get_filename(Job, Idx) ->
    lists:append([get_prefix(Job, Idx), atom_to_list(node()), ".tmp"]).

get_prefix(Job, Idx) ->
    lists:append([?TMP, Job#job.clustername, "_", Job#job.name, "_",
                  integer_to_list(Idx), "_"]).

get_files(Job, Idx) ->
    filelib:wildcard(get_prefix(Job, Idx)++"*.tmp").

get_lock(FileName) ->
    Lock = FileName ++ ".lock",
    create_lock(Lock).

create_lock(Lock) ->
    case filelib:is_file(Lock) of
        false ->
            {ok, Fd} = file:open(Lock, write),
            io:format(Fd, "~p", [node()]),
            ok = file:close(Fd),
            Lock;
        true ->
            timer:sleep(100),
            create_lock(Lock)
    end.

remove_lock(Lock) ->
    ok = file:delete(Lock).

get_max_tab_size(Job) ->
    %%assume each node can hold up to 400M ets tables
    Size = 400*1024*1024 div Job#job.partition,
    if Size > 6*1024*1024 ->
            6*1024*1024;
       true -> Size
    end.

remove_possible_lock(Owner) ->
    lists:foreach(
        fun(F) -> 
           case file:read_file(F) of
               {ok, Bin} ->
                   case binary_to_list(Bin) of
                       Owner ->
                           ?LOG("remove lock for "++Owner),
                           file:delete(F);
                       _ -> ok
                   end;
               _ -> ok end
        end, filelib:wildcard(?TMP++"*.lock")).
