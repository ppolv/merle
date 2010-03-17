-module(merle_watcher).

-export([start_link/1, init/1, handle_call/3, handle_info/2, handle_cast/2, terminate/2]).

-define(RESTART_INTERVAL, 15 * 1000). %% retry each 5 seconds. 

-record(state, {mcd_pid, 
                host,
                port}).

start_link([Host, Port]) ->
    gen_server:start_link(?MODULE, [Host, Port], []).

init([Host, Port]) ->
   erlang:process_flag(trap_exit, true),
   self() ! timeout,
   {ok, #state{mcd_pid = undefined, host = Host, port = Port}}.

handle_call(_Call, _From, S) ->
    {reply, ok, S}.
handle_info('timeout', #state{mcd_pid = undefined, host = Host, port = Port} = State) ->
   error_logger:info_report([{memcached, connecting}, {host, Host}, {port, Port}]),
   case merle:connect(Host, Port) of
        {ok, Pid} ->
           local_pg2:create({Host, Port}),
           local_pg2:join({Host, Port}, Pid),
           {noreply, State#state{mcd_pid = Pid}};
        {error, Reason} ->
	    receive 
		{'EXIT', _ , _} ->
			ok
		after
		        2000 ->
			ok
	    end,
	    error_logger:error_report([memcached_not_started,
                               {reason, Reason},
                               {host, Host},
                               {port, Port},
                              {restarting_in, ?RESTART_INTERVAL}]),
            {noreply, State, ?RESTART_INTERVAL}
   end;
	
handle_info({'EXIT', Pid, Reason}, #state{mcd_pid = Pid} = S) ->
    error_logger:error_report([{memcached_crashed, Pid},
                               {reason, Reason},
                               {host, S#state.host},
                               {port, S#state.port},
                              {restarting_in, ?RESTART_INTERVAL}]),
    {noreply, S#state{mcd_pid = undefined}, ?RESTART_INTERVAL};
handle_info(_Info, S) ->
    error_logger:warning_report([{merle_watcher, self()}, {unknown_info, _Info}]),
    case S#state.mcd_pid of
	undefined ->
	    {noreply, S, ?RESTART_INTERVAL};
	_ ->
	    {noreply, S}
    end.
handle_cast(_Cast, S) ->
    {noreply, S}.
terminate(_Reason, _S) ->
    ok.



