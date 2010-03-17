-module(local_pg2).

%% Basically the same functionality than pg2,  but process groups are local rather than global.
-export([create/1, delete/1, join/2, leave/2, get_members/1, get_closest_pid/1, which_groups/0]).

-export([start/0, start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TABLE, local_pg2_table).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start() ->
    ensure_started().

create(Name) ->
    ensure_started(),
    case ets:lookup(?TABLE, Name) of
        [] ->
            gen_server:call(?MODULE, {create, Name});
        _ ->
            ok
    end.
delete(Name) ->
    ensure_started(),
    gen_server:call(?MODULE, {delete, Name}).

join(Name, Pid) when is_pid(Pid) ->
    ensure_started(),
    case ets:lookup(?TABLE, Name) of
        [] ->
            {error, {no_such_group, Name}};
        _ ->
            gen_server:call(?MODULE, {join, Name, Pid})
    end.
leave(Name, Pid) when is_pid(Pid) ->
    ensure_started(),
    case ets:lookup(?TABLE, Name) of
        [] ->
            {error, {no_such_group, Name}};
        _ ->
            gen_server:call(?MODULE, {leave, Name, Pid})
    end.

get_members(Name) ->
    ensure_started(),
    case ets:lookup(?TABLE, Name) of
        [] -> {error, {no_such_group, Name}};
        [{Name, Members}] -> Members
     end.
which_groups() ->
    ensure_started(),
    [K || {K, _Members} <- ets:tab2list(?TABLE)].

    
get_closest_pid(Name) ->
    ensure_started(),
    case ets:lookup(?TABLE, Name) of
        [] ->
            {error, {no_process, Name}};
        [{Name, Members}] ->
            %% TODO:  we can get more inteligent, check queue size, reductions, etc.
            %% http://lethain.com/entry/2009/sep/12/load-balancing-across-erlang-process-groups/
            {_, _, X} = erlang:now(),
            lists:nth((X rem length(Members)) +1, Members)
    end.


init([]) ->
    process_flag(trap_exit, true),
    ets:new(?TABLE, [set, protected, named_table]),
    {ok, []}.
handle_call({create, Name}, _From, S) ->
    case ets:lookup(?TABLE, Name) of
        [] ->
            ets:insert(?TABLE, {Name, []});
        _ ->
            ok
    end,
    {reply, ok, S};

handle_call({join, Name, Pid}, _From, S) ->
    case ets:lookup(?TABLE, Name) of
        [] ->
            {reply, no_such_group, S};
        [{Name, Members}] ->
            ets:insert(?TABLE, {Name, [Pid | Members]}),
            link(Pid),
            %%TODO: add pid to linked ones on state..
            {reply, ok, S}
    end;
            
handle_call({leave, Name, Pid}, _From, S) ->
    case ets:lookup(?TABLE, Name) of
        [] ->
            {reply, no_such_group, S};
        [{Name, Members}] ->
            case lists:delete(Pid, Members) of
                [] ->
                    ets:delete(?TABLE, Name);
                NewMembers ->
                    ets:insert(?TABLE, {Name, NewMembers})
            end,
            unlink(Pid),
            {reply, ok, S}
     end;

handle_call({delete, Name}, _From, S) ->
    ets:delete(?TABLE, Name),
    {reply, ok, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({'EXIT', Pid, _} , S) ->
    del_member(Pid),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ets:delete(?TABLE),
    %%do not unlink, if this fails, dangling processes should be killed
    ok.
%%%-----------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------
del_member(Pid) ->
    L = ets:tab2list(?TABLE),
    lists:foreach(fun({Name, Members}) ->
                      case lists:member(Pid, Members) of
                            true ->
                                case lists:delete(Pid, Members) of
                                    [] ->
                                        ets:delete(?TABLE, Name);
                                    NewMembers ->
                                        ets:insert(?TABLE, {Name, NewMembers})
                                end;
                            false ->
                                ok
                       end
                   end, L).

ensure_started() ->
    case whereis(?MODULE) of
	undefined ->
	    C = {local_pg2, {?MODULE, start_link, []}, permanent,
		 1000, worker, [?MODULE]},
	    supervisor:start_child(kernel_safe_sup, C);
 	Pg2Pid ->
	    {ok, Pg2Pid}
    end.

