-module(yamr_job).

-export([init/0,
         put_job/1,
         put_job/3,
         get_job/1,
         remove_job/1,
         get_clustername/1,
         put_task/1,
         get_a_task_except/2,
         get_a_task/2]).

-include("yamr.hrl").
-define(JOBID, 'yamr_job_id').

-define(JOB_TAB, yamr_job_tab).
-define(YASK_TAB, yamr_yask_queue_tab).

-define(ERR_RET, "FAILED TO READ JOB FILE").
-define(ERR_RET1, "READ JOB FILE TIMEOUT").

-define(M, map_task).
-define(R, raw_tasks).
%% yask is the task in mixed yamr task queue, it could be either one
%% map task or a batch of raw tasks
-record(yask, {prio::integer(),
               jobname::string(),
               type=?M::?M|?R,
               task=undefined::#map_task{}|undefined}).

init() ->
    put(?JOBID, 1),
    ?JOB_TAB = ets:new(?JOB_TAB, [named_table, set, public]),
    ?YASK_TAB = ets:new(?YASK_TAB, [named_table, set, public]),
    true = ets:insert(?YASK_TAB, {yask, []}).

put_job([ClusterName, CBModule, Language, CodePath, Vsn, Prio,
         Partition, JobFile], From, NoOfTasks2Fetch) ->
    JobId = get(?JOBID),
    Job = #job{name=get_a_jobname(ClusterName, JobId),
               clustername=ClusterName,
               cb_module=CBModule,
               code_path=CodePath,
               language=Language,
               vsn=list_to_integer(Vsn),
               priority=list_to_integer(Prio),
               partition=list_to_integer(Partition),
               jobfile=JobFile,
               progress_index=0,
               client=From},
    case get_init_tasks(Job, NoOfTasks2Fetch) of
        {ok, Tasks, NewJob} ->
            NxtJobId = if JobId =:= 9999999 -> 1; true -> JobId+1 end,
            put(?JOBID, NxtJobId),
            Yask = #yask{prio = Job#job.priority,
                         jobname = Job#job.name},
            Yasks = [Yask#yask{type=?M, task=T}||T<-Tasks],
            put_job(NewJob),
            put_yasks(lists:append(Yasks, [Yask#yask{type=?R}]),Yask#yask.prio),
            ok;
        Error ->
            Error
    end.

put_task(Task) ->
    Yask = #yask{prio = Task#map_task.priority,
                 jobname = Task#map_task.jobname,
                 type = ?M, task=Task},
    put_yasks([Yask], Yask#yask.prio).

put_job(Job) when is_record(Job, job) ->
    true = ets:insert(?JOB_TAB, {Job#job.name, Job}).

remove_job(JobName) ->
    true = ets:delete(?JOB_TAB, JobName).

get_clustername(JobName) ->
    case get_job(JobName) of
        not_found -> job_not_found;
        Job -> Job#job.clustername
    end.

get_job(JobName) ->
    case ets:lookup(?JOB_TAB, JobName) of
        [] -> not_found;
        [{_, Job}] -> Job
    end.

get_init_tasks(Job, NoOfTasks) ->
    Pid = spawn(fun()-> get_tasks_from_file(Job, NoOfTasks) end),
    receive
        {ok, Tasks, NewJob} ->
            {ok, Tasks, NewJob};
        {error, Error} ->
            {error, Error}
    after 5000 -> exit(Pid, kill), {error, ?ERR_RET1} end.

get_tasks_from_file(Job, NoOfTasks) ->
    try {Tasks, NewJob} =
        if Job#job.progress_index =:= -1 ->
              {[], Job#job{progress_index = -1}};
           true ->
              {ok, Bin} = file:read_file(Job#job.jobfile),
              Data = string:tokens(binary_to_list(Bin), "\n"),
              Index = Job#job.progress_index,
              {_, Left} = lists:split(Index, Data),
              {Tasks1, NewIndex} =
              if NoOfTasks >= length(Left) ->
                    {Left, -1};
                 true ->
                    {Tks, _} = lists:split(NoOfTasks, Left),
                    {Tks, Index+NoOfTasks}
              end,
              {Tasks1, Job#job{number_of_tasks=length(Data), 
                               progress_index=NewIndex}}
        end,
        ?MASTER ! {ok,
                   [#map_task{jobname=Job#job.name, 
                              clustername=Job#job.clustername, 
                              priority=Job#job.priority, 
                              partition=Job#job.partition,
                              task=T}||T<-Tasks], NewJob}
    catch _:_ -> ?MASTER ! {error, ?ERR_RET} end.

get_yasks() ->
    [{yask, Yasks}] = ets:lookup(?YASK_TAB, yask),
    Yasks.

put_yasks(Yasks) ->
    true = ets:insert(?YASK_TAB, {yask, Yasks}).

put_yasks(Yasks, Prio) ->
    NewYasks =
    case get_yasks() of
        [] -> Yasks;
        All -> 
            %% higher number lower priority
            {High, Low} = split_yasks_by_prio(All, Prio),
            lists:append([High, Yasks, Low])
    end,
    put_yasks(NewYasks).

get_a_task_except(InitNum, JobBlacklist) ->
    case [Y||Y<-get_yasks(), not(lists:member(Y#yask.jobname, JobBlacklist))] of
        [] -> [];
        [Yask|Rest] ->
           {Task, Yasks} = 
           case Yask of
               Yask when Yask#yask.type == ?M ->
                   {Yask#yask.task, Rest};
               Yask ->
                   {ok, Tasks, NewJob} = 
                   get_init_tasks(get_job(Yask#yask.jobname), InitNum),
                   put_job(NewJob),
                   if Tasks =:= [] -> {[], Rest};
                      true -> 
                          [Tasks1|TasksLeft] = Tasks,
                          Yasks1=[#yask{prio=T#map_task.priority,
                                        jobname=T#map_task.jobname,
                                        type=?M,task=T}||T<-TasksLeft],
                          {Tasks1, lists:append([Yasks1, [Yask], Rest])}
                   end
           end,
           put_yasks(Yasks),
           Task
    end.

get_a_task(JobName, InitNum) ->
    case get_yasks() of
        [] -> [];
        Yasks ->
            case [Y||Y<-Yasks, Y#yask.jobname =:= JobName] of
                [] -> [];
                [Yask|Rest] ->
                    put_yasks(Yasks -- [Yask]),
                    if Yask#yask.type =:= ?M ->
                         Yask#yask.task;
                       true ->
                         {ok, Tasks, NewJob} = 
                         get_init_tasks(get_job(Yask#yask.jobname), InitNum),
                         put_job(NewJob),
                         {RetTask, NewYasks} =
                         if Tasks =:= [] -> {[], Rest};
                            true -> 
                                [Tasks1|TasksLeft] = Tasks,
                                Yasks1 = [#yask{prio=T#map_task.priority,
                                                jobname=T#map_task.jobname,
                                                type=?M, task=T}||T<-TasksLeft],
                                {Tasks1, lists:append(Yasks1, [Yask])}
                         end,
                         put_yasks(NewYasks, Yask#yask.prio),
                         RetTask
                    end
            end
    end.

split_yasks_by_prio(Yasks, Prio) ->
    {[Y||Y<-Yasks, Prio >= Y#yask.prio], [Y||Y<-Yasks, Prio < Y#yask.prio]}.

get_a_jobname(ClusterName, JobId) ->
    ClusterName++"-"++integer_to_list(JobId).

