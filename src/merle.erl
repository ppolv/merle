%% Copyright 2009, Joe Williams <joe@joetify.com>
%% Copyright 2009, Nick Gerakines <nick@gerakines.net>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
%%
%% @author Joseph Williams <joe@joetify.com>
%% @copyright 2008 Joseph Williams
%% @version 0.3
%% @seealso http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
%% @doc An Erlang memcached client.
%%
%% This code is available as Open Source Software under the MIT license.
%%
%% Updates at http://github.com/joewilliams/merle/

-module(merle).
-behaviour(gen_server2).

-author("Joe Williams <joe@joetify.com>").
-version("Version: 0.3").

-define(TIMEOUT, 5000).
-define(RANDOM_MAX, 65535).
-define(DEFAULT_HOST, "localhost").
-define(DEFAULT_PORT, 11211).
-define(TCP_OPTS_ACTIVE, [
	binary, {packet, line}, {nodelay, true}, {reuseaddr, true}, {active, once}]).
-define(TCP_OPTS_LINE, [
    binary, {packet, line}, {nodelay, true},{reuseaddr, true}, {active, false}
]).
-define(TCP_OPTS_RAW, [
    binary, {packet, raw}, {nodelay, true},{reuseaddr, true}, {active, false}
]).

%% gen_server API
-export([
    stats/1, stats/2, version/1, getkey/2, getkeys/2, delete/3, set/5, add/5, replace/3,
    replace/5, cas/6, set/3, flushall/1, flushall/2, verbosity/2, add/3,
    cas/4, getskey/2, connect/0, connect/2, delete/2, disconnect/1
]).

%% gen_server callbacks
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3
]).

%% @doc retrieve memcached stats
stats(Ref) ->
	gen_server2:call(Ref, {stats}).

%% @doc retrieve memcached stats based on args
stats(Ref, Args) when is_atom(Args)->
	stats(Ref, atom_to_list(Args));
stats(Ref, Args) ->
	gen_server2:call(Ref, {stats, {Args}}).

%% @doc retrieve memcached version
version(Ref) ->
	gen_server2:call(Ref, {version}).

%% @doc set the verbosity level of the logging output
verbosity(Ref, Args) when is_integer(Args) ->
	verbosity(Ref, integer_to_list(Args));
verbosity(Ref, Args)->
	gen_server2:call(Ref, {verbosity, {Args}}).

%% @doc invalidate all existing items immediately
flushall(Ref) ->
	gen_server2:call(Ref, {flushall}).

%% @doc invalidate all existing items based on the expire time argument
flushall(Ref, Delay) when is_integer(Delay) ->
	flushall(Ref, integer_to_list(Delay));
flushall(Ref, Delay) ->
	gen_server2:call(Ref, {flushall, {Delay}}).

%% @doc retrieve value based off of key
getkey(Ref, Key) when is_atom(Key) ->
	getkey(Ref, atom_to_list(Key));
getkey(Ref, Key) ->
	gen_server2:call(Ref, {getkey,{Key}}).

%% @doc retrieve multiple values based on keys
getkeys(Ref, Keys) when is_list(Keys) ->
	StringKeys = lists:map(fun
			(A) when is_atom(A) -> 
				atom_to_list(A);
			(S) ->
				S
			end, Keys),
	
	gen_server2:call(Ref, {getkeys,{join_by(StringKeys, " ")}}).


%% @doc retrieve value based off of key for use with cas
getskey(Ref, Key) when is_atom(Key) ->
	getskey(Ref, atom_to_list(Key));
getskey(Ref, Key) ->
	gen_server2:call(Ref, {getskey,{Key}}).

%% @doc delete a key
delete(Ref, Key) ->
	delete(Ref, Key, "0").

delete(Ref, Key, Time) when is_atom(Key) ->
	delete(Ref, atom_to_list(Key), Time);
delete(Ref, Key, Time) when is_integer(Time) ->
	delete(Ref, Key, integer_to_list(Time));
delete(Ref, Key, Time) ->
	gen_server2:call(Ref, {delete, {Key, Time}}).

%% Time is the amount of time in seconds
%% the client wishes the server to refuse
%% "add" and "replace" commands with this key.

%%
%% Storage Commands
%%

%% *Flag* is an arbitrary 16-bit unsigned integer (written out in
%% decimal) that the server stores along with the Value and sends back
%% when the item is retrieved.
%%
%% *ExpTime* is expiration time. If it's 0, the item never expires
%% (although it may be deleted from the cache to make place for other
%%  items).
%%
%% *CasUniq* is a unique 64-bit value of an existing entry.
%% Clients should use the value returned from the "gets" command
%% when issuing "cas" updates.
%%
%% *Value* is the value you want to store.

%% @doc Store a key/value pair.
set(Ref, Key, Value) ->
    Flag = random:uniform(?RANDOM_MAX),
    set(Ref, Key, integer_to_list(Flag), "0", Value).

set(Ref, Key, Flag, ExpTime, Value) when is_atom(Key) ->
	set(Ref, atom_to_list(Key), Flag, ExpTime, Value);
set(Ref, Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    set(Ref, Key, integer_to_list(Flag), ExpTime, Value);
set(Ref, Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    set(Ref, Key, Flag, integer_to_list(ExpTime), Value);
set(Ref, Key, Flag, ExpTime, Value) ->
	gen_server2:call(Ref, {set, {Key, Flag, ExpTime, Value}}).

%% @doc Store a key/value pair if it doesn't already exist.
add(Ref, Key, Value) ->
	Flag = random:uniform(?RANDOM_MAX),
	add(Ref, Key, integer_to_list(Flag), "0", Value).

add(Ref, Key, Flag, ExpTime, Value) when is_atom(Key) ->
	add(Ref, atom_to_list(Key), Flag, ExpTime, Value);
add(Ref, Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    add(Ref, Key, integer_to_list(Flag), ExpTime, Value);
add(Ref, Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    add(Ref, Key, Flag, integer_to_list(ExpTime), Value);
add(Ref, Key, Flag, ExpTime, Value) ->
	gen_server2:call(Ref, {add, {Key, Flag, ExpTime, Value}}).

%% @doc Replace an existing key/value pair.
replace(Ref, Key, Value) ->
	Flag = random:uniform(?RANDOM_MAX),
	replace(Ref, Key, integer_to_list(Flag), "0", Value).

replace(Ref, Key, Flag, ExpTime, Value) when is_atom(Key) ->
	replace(Ref, atom_to_list(Key), Flag, ExpTime, Value);
replace(Ref, Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    replace(Ref, Key, integer_to_list(Flag), ExpTime, Value);
replace(Ref, Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    replace(Ref, Key, Flag, integer_to_list(ExpTime), Value);
replace(Ref, Key, Flag, ExpTime, Value) ->
	gen_server2:call(Ref, {replace, {Key, Flag, ExpTime, Value}}).

%% @doc Store a key/value pair if possible.
cas(Ref, Key, CasUniq, Value) ->
	Flag = random:uniform(?RANDOM_MAX),
	cas(Ref, Key, integer_to_list(Flag), "0", CasUniq, Value).

cas(Ref, Key, Flag, ExpTime, CasUniq, Value) when is_atom(Key) ->
	cas(Ref, atom_to_list(Key), Flag, ExpTime, CasUniq, Value);
cas(Ref, Key, Flag, ExpTime, CasUniq, Value) when is_integer(Flag) ->
    cas(Ref, Key, integer_to_list(Flag), ExpTime, CasUniq, Value);
cas(Ref, Key, Flag, ExpTime, CasUniq, Value) when is_integer(ExpTime) ->
    cas(Ref, Key, Flag, integer_to_list(ExpTime), CasUniq, Value);
cas(Ref, Key, Flag, ExpTime, CasUniq, Value) when is_integer(CasUniq) ->
    cas(Ref, Key, Flag, ExpTime, integer_to_list(CasUniq), Value);
cas(Ref, Key, Flag, ExpTime, CasUniq, Value) ->
	gen_server2:call(Ref, {cas, {Key, Flag, ExpTime, CasUniq, Value}}).

%% @doc connect to memcached with defaults
connect() ->
	connect(?DEFAULT_HOST, ?DEFAULT_PORT).

%% @doc connect to memcached
connect(Host, Port) ->
	start_link(Host, Port).

%% @doc disconnect from memcached
disconnect(Ref) ->
	gen_server2:call(Ref, {stop}),
	ok.

%% @private
start_link(Host, Port) ->
    gen_server2:start_link(?MODULE, [Host, Port], []).

%% @private
init([Host, Port]) ->
    gen_tcp:connect(Host, Port, ?TCP_OPTS_ACTIVE).

handle_call({stop}, _From, Socket) ->
    {stop, requested_disconnect, Socket};

handle_call({stats}, _From, Socket) ->
    Reply = send_stats_cmd(Socket, iolist_to_binary([<<"stats">>])),
    {reply, Reply, Socket};

handle_call({stats, {Args}}, _From, Socket) ->
    Reply = send_stats_cmd(Socket, iolist_to_binary([<<"stats ">>, Args])),
    {reply, Reply, Socket};

handle_call({version}, _From, Socket) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"version">>])),
    {reply, Reply, Socket};

handle_call({verbosity, {Args}}, _From, Socket) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"verbosity ">>, Args])),
    {reply, Reply, Socket};

handle_call({flushall}, _From, Socket) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"flush_all">>])),
    {reply, Reply, Socket};

handle_call({flushall, {Delay}}, _From, Socket) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"flush_all ">>, Delay])),
    {reply, Reply, Socket};

handle_call({getkey, {Key}}, _From, Socket) ->
    Reply = send_get_cmd(Socket, iolist_to_binary([<<"get ">>, Key])),
    {reply, Reply, Socket};
handle_call({getkeys, {Keys}}, _From, Socket) ->
    Reply = send_multi_get_cmd(Socket, iolist_to_binary([<<"get ">>, Keys])),
    {reply, Reply, Socket};

handle_call({getskey, {Key}}, _From, Socket) ->
    Reply = send_gets_cmd(Socket, iolist_to_binary([<<"gets ">>, Key])),
    {reply, [Reply], Socket};

handle_call({delete, {Key, Time}}, _From, Socket) ->
    Reply = send_generic_cmd(
        Socket,
        iolist_to_binary([<<"delete ">>, Key, <<" ">>, Time])
    ),
    {reply, Reply, Socket};

handle_call({set, {Key, Flag, ExpTime, Value}}, _From, Socket) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(
        Socket,
        iolist_to_binary([
            <<"set ">>, Key, <<" ">>, Flag, <<" ">>, ExpTime, <<" ">>, Bytes
        ]),
        Bin
    ),
    {reply, Reply, Socket};

handle_call({add, {Key, Flag, ExpTime, Value}}, _From, Socket) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(
        Socket,
        iolist_to_binary([
            <<"add ">>, Key, <<" ">>, Flag, <<" ">>, ExpTime, <<" ">>, Bytes
        ]),
        Bin
    ),
    {reply, Reply, Socket};

handle_call({replace, {Key, Flag, ExpTime, Value}}, _From, Socket) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(
        Socket,
        iolist_to_binary([
            <<"replace ">>, Key, <<" ">>, Flag, <<" ">>, ExpTime, <<" ">>,
            Bytes
        ]),
    	Bin
    ),
    {reply, Reply, Socket};

handle_call({cas, {Key, Flag, ExpTime, CasUniq, Value}}, _From, Socket) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(
        Socket,
        iolist_to_binary([
            <<"cas ">>, Key, <<" ">>, Flag, <<" ">>, ExpTime, <<" ">>, Bytes,
            <<" ">>, CasUniq
        ]),
        Bin
    ),
    {reply, Reply, Socket}.

%% @private
handle_cast(_Msg, State) -> {noreply, State}.

%% @private
handle_info({tcp_closed, Socket}, Socket) -> 
    {stop, {error, tcp_closed}, Socket};
handle_info({tcp_error, Socket, Reason}, Socket) -> 
    {stop, {error, {tcp_error, Reason}}, Socket};
handle_info(_Info, State) -> {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @private
%% @doc Closes the socket
terminate(_Reason, Socket) ->
    gen_tcp:close(Socket),
    ok.

%% @private
%% @doc send_stats_cmd/2 function for stats get
send_stats_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    Reply = recv_stats(),
    Reply.

%% @private
%% @doc send_generic_cmd/2 function for simple informational and deletion commands
send_generic_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
	Reply = recv_simple_reply(),
	Reply.

%% @private
%% @doc send_storage_cmd/3 funtion for storage commands
send_storage_cmd(Socket, Cmd, Value) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    gen_tcp:send(Socket, <<Value/binary, "\r\n">>),
    Reply = recv_simple_reply(),
   	Reply.

%% @private
%% @doc send_get_cmd/2 function for retreival commands
send_get_cmd(Socket, Cmd) ->
    inet:setopts(Socket, ?TCP_OPTS_LINE),
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    Reply = case recv_complex_get_reply(Socket) of
		[{_, Value}] -> {ok, Value};
		[] -> {error, not_found};
		{error, Error} -> {error, Error}
    	    end,
    inet:setopts(Socket, ?TCP_OPTS_ACTIVE),
    Reply.

send_multi_get_cmd(Socket, Cmd) ->
    inet:setopts(Socket, ?TCP_OPTS_LINE),
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    Reply = case recv_complex_get_reply(Socket) of
		{error, Error} -> {error, Error};
		R -> {ok, R}
	    end,
    inet:setopts(Socket, ?TCP_OPTS_ACTIVE),
    Reply.
	
%% @private
%% @doc send_gets_cmd/2 function for cas retreival commands
send_gets_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
	Reply = recv_complex_gets_reply(Socket),
	Reply.

%% @private


%% {active, once} is overkill here, but don't worry to much on optimize this method
recv_stats() ->
	case do_recv_stats() of
		timeout -> {error, timeout};
		Stats -> {ok, Stats}
	end.
do_recv_stats() ->
    receive
        {tcp, Socket, <<"END\r\n">>} ->
            inet:setopts(Socket, ?TCP_OPTS_ACTIVE),
            [];
        {tcp, Socket, Data} ->
  			{ok, [Field, Value], []} = io_lib:fread("STAT ~s ~s \r\n", binary_to_list(Data)),
            inet:setopts(Socket, ?TCP_OPTS_ACTIVE),  
            [{Field, Value} | do_recv_stats()]
     after ?TIMEOUT ->
	timeout
   end.
%% @doc receive function for simple responses (not containing VALUEs)
recv_simple_reply() ->
	receive
	  	{tcp, Socket, Data} ->
        	inet:setopts(Socket, ?TCP_OPTS_ACTIVE),
        	parse_simple_response_line(Data); 
        {error, closed} ->
  			connection_closed
    after ?TIMEOUT -> {error, timeout}
    end.
parse_simple_response_line(<<"OK", _B/binary>>) -> ok;
parse_simple_response_line(<<"ERROR", _B/binary>> =L ) -> {error, L};
parse_simple_response_line(<<"CLIENT_ERROR", _B/binary>> =L ) -> {error, L};
parse_simple_response_line(<<"SERVER_ERROR", _B/binary>> =L) -> {error, L};
parse_simple_response_line(<<"STORED", _B/binary>>) -> ok;
parse_simple_response_line(<<"NOT_STORED", _B/binary>> ) -> ok;
parse_simple_response_line(<<"EXISTS", _B/binary>> ) -> {error, exists};
parse_simple_response_line(<<"NOT_FOUND", _B/binary>> ) -> {error, not_found};
parse_simple_response_line(<<"DELETED", _B/binary>> ) -> ok;
parse_simple_response_line(<<"VERSION", _B/binary>> =L) -> {ok, L};
parse_simple_response_line(Line) -> {error, {unknown_response, Line}}.


%% @private
%% @doc receive function for respones containing VALUEs
recv_complex_get_reply(Socket) ->
	recv_complex_get_reply(Socket, []).
recv_complex_get_reply(Socket, Accum) ->
	case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
		{ok, <<"END\r\n">>} -> 
			Accum;
		{ok, Data} ->
  			{ok,[_,Key,_,Bytes], []} = 
				io_lib:fread("~s ~s ~u ~u\r\n", binary_to_list(Data)),
            		inet:setopts(Socket, ?TCP_OPTS_RAW),
			case  gen_tcp:recv(Socket, Bytes+2, ?TIMEOUT) of
				{ok, <<Value:Bytes/binary, "\r\n">>} -> 
					inet:setopts(Socket, ?TCP_OPTS_LINE),
					recv_complex_get_reply(Socket, 
						[{Key, binary_to_term(Value)}|Accum]);
				{error, Error} ->
					{error, Error}
			end;
		{error, Error} ->
			{error, Error}
	end.
		

%% @private
%% @doc receive function for cas responses containing VALUEs
recv_complex_gets_reply(Socket) ->
	receive
		%% For receiving get responses where the key does not exist
		{tcp, Socket, <<"END\r\n">>} -> 
        inet:setopts(Socket, ?TCP_OPTS_LINE),
        {error, not_found};
		%% For receiving get responses containing data
		{tcp, Socket, Data} ->
			%% Reply format <<"VALUE SOMEKEY FLAG BYTES\r\nSOMEVALUE\r\nEND\r\n">>
  			Parse = io_lib:fread("~s ~s ~u ~u ~u\r\n", binary_to_list(Data)),
  			{ok,[_,_,_,Bytes,CasUniq], []} = Parse,
  			Reply = get_data(Socket, Bytes),
  			{ok, [CasUniq, Reply]};
  		{error, closed} ->
  			{error, connection_closed}
    after ?TIMEOUT -> {error, timeout}
    end.

%% @private
%% @doc recieve loop to get all data
get_data(Socket, Bytes) ->
    inet:setopts(Socket, ?TCP_OPTS_RAW),
    {ok, Data} = gen_tcp:recv(Socket, Bytes+7, ?TIMEOUT),
    <<Value:Bytes/binary, "\r\nEND\r\n">> = Data,
    inet:setopts(Socket, ?TCP_OPTS_ACTIVE),
    binary_to_term(Value).

%% @private
join_by([], _) ->
	[];
join_by([A|[]], _) ->
	[A];
join_by([A|Rest], J) ->
	[A, J | join_by(Rest, J)].

