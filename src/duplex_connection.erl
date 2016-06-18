-module(duplex_connection).

-behaviour(gen_statem).

-export([start_link/3,
         send_recv/2,
         send_recv/3]).

-export([init/1,
         backoff/3,
         await/3,
         half_duplex/3,
         semi_duplex/3,
         full_duplex/3,
         closing/3,
         code_change/4,
         terminate/3]).

-callback init(Arg) ->
    {Mode, Info, Broker} | {stop, Reason} | ignore when
      Arg :: term(),
      Mode :: half_duplex | full_duplex,
      Info :: term(),
      Broker :: sbroker:broker(),
      Reason :: term().

-callback connect(Info) ->
    {ok, Buffer, Socket} | {error, Reason} when
      Info :: term(),
      Buffer :: term(),
      Socket :: term(),
      Reason :: term().

-callback send(Req, Socket) ->
    {send_recv | recv, NReq} | {result, Result} | {close, Reason} when
      Req :: term(),
      Socket :: term(),
      NReq :: term(),
      Result :: term(),
      Reason :: term().

-callback send_recv(Req, Buffer, Socket) ->
    {recv, NReq, NBuffer} | {result, Result, NBuffer} | {close, Reason} when
      Req :: term(),
      Buffer :: term(),
      Socket :: term(),
      NReq :: term(),
      NBuffer :: term(),
      Result :: term(),
      Reason :: term().

-callback recv(Req, Buffer, Socket) ->
    {result, Result, NBuffer} | {close, Reason} when
      Req :: term(),
      Buffer :: term(),
      Socket :: term(),
      Result :: term(),
      NBuffer :: term(),
      Reason :: term().

-callback close(Reason, Socket) -> term() when
      Reason :: term(),
      Socket :: term().

-optional_callbacks([send_recv/3, recv/3]).

-record(client, {ref :: reference(),
                 pid :: pid(),
                 mon :: reference() | 'DOWN',
                 timer :: reference() | infinity}).

-record(data, {mod :: module(),
               args :: term(),
               info :: term(),
               socket=undefined :: term(),
               buffer=undefined :: term(),
               mode :: half_duplex | full_duplex,
               broker :: pid() | {atom(), node()},
               broker_mon :: reference(),
               broker_ref=make_ref() :: reference(),
               send=undefined :: undefined | #client{} | unknown,
               recv=undefined :: undefined | #client{}}).

start_link(Mod, Args, Opts) ->
    gen_statem:start_link(?MODULE, {Mod, Args}, Opts).

send_recv(Broker, Req) ->
    send_recv(Broker, Req, infinity).

send_recv(Broker, Req, Timeout) ->
    case sbroker:ask(Broker, {self(), Timeout}) of
        {go, Ref, {Mod, Socket, Conn}, _, _} ->
            send(Mod, Req, Socket, Conn, Ref);
        {drop, _} = Drop ->
            {error, Drop}
    end.

init({Mod, Args}) ->
    try Mod:init(Args) of
        Result ->
            handle_init(Result, Mod, Args)
    catch
        throw:Result ->
            handle_init(Result, Mod, Args)
    end.

backoff(internal, connect, #data{mod=Mod, info=Info} = Data) ->
    try Mod:connect(Info) of
        Result ->
            handle_connect(Result, Data)
    catch
        throw:Result ->
            handle_connect(Result, Data)
    end.

await(info, {BRef, {go, Ref, {Pid, Timeout}, _, SojournTime}},
      #data{broker_ref=BRef, mode=Mode, broker=Broker, buffer=Buffer} = Data) ->
    continue(Pid, Ref, Mode, Broker, Buffer),
    Client = start_client(Ref, Pid, Timeout, SojournTime),
    NData = Data#data{broker_ref=Ref, send=Client, recv=Client,
                      buffer=undefined},
    case Mode of
        half_duplex ->
            {next_state, half_duplex, NData};
        full_duplex ->
            {next_state, semi_duplex, NData}
    end;
await(Type, Event, Data) ->
    handle_event(Type, Event, await, Data).

half_duplex(cast, {done, Ref, Buffer},
        #data{send=#client{ref=Ref} = Client} = Data) ->
    NData = ask_r(Data#data{send=undefined, recv=undefined, buffer=Buffer}),
    cancel_client(Client),
    {next_state, await, NData};
half_duplex(cast, {close, Ref, Reason},
            #data{send=#client{ref=Ref} = Client} = Data) ->
    cancel_client(Client),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next(Reason)};
half_duplex(info, {timeout, TRef, {Pid, Timeout}},
            #data{send=#client{timer=TRef} = Client} = Data)
  when is_reference(TRef) ->
    cancel_client(Client#client{timer=infinity}),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({timeout, Pid, Timeout})};
half_duplex(info, {'DOWN', MRef, _, Pid, Reason},
        #data{send=#client{mon=MRef} = Client} = Data) ->
    cancel_client(Client#client{mon='DOWN'}),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({'DOWN', Pid, Reason})};
half_duplex(Type, Event, Data) ->
    handle_event(Type, Event, half_duplex, Data).

semi_duplex(info, {BRef, {go, Ref, {Pid, Timeout}, _, SojournTime}},
            #data{broker_ref=BRef} = Data) ->
    Send = start_client(Ref, Pid, Timeout, SojournTime),
    {next_state, full_duplex, Data#data{send=Send, broker_ref=Ref}};
semi_duplex(cast, {done, Ref, Buffer},
            #data{send=#client{ref=Ref} = Client} = Data) ->
    cancel_client(Client),
    NData = Data#data{send=undefined, recv=undefined, buffer=Buffer},
    {next_state, await, NData};
semi_duplex(cast, {close, Ref, Reason},
            #data{send=#client{ref=Ref} = Client} = Data) ->
    cancel_client(Client),
    NData = Data#data{send=unknown, recv=undefined},
    {next_state, closing, NData, close_next(Reason)};
semi_duplex(info, {timeout, TRef, {Pid, Timeout}},
            #data{send=#client{timer=TRef} = Client} = Data)
  when is_reference(TRef) ->
    cancel_client(Client#client{timer=infinity}),
    NData = Data#data{send=unknown, recv=undefined},
    {next_state, closing, NData, close_next({timeout, Pid, Timeout})};
semi_duplex(info, {'DOWN', MRef, _, Pid, Reason},
        #data{send=#client{mon=MRef} = Client} = Data) ->
    cancel_client(Client#client{mon='DOWN'}),
    NData = Data#data{send=unknown, recv=undefined},
    {next_state, closing, NData, close_next({'DOWN', Pid, Reason})};
semi_duplex(Type, Event, Data) ->
    handle_event(Type, Event, semi_duplex, Data).

full_duplex(cast, {done, Ref, Buffer},
            #data{recv=#client{ref=Ref} = Recv, send=Send,
                  broker=Broker} = Data) ->
    continue(Send, full_duplex, Broker, Buffer),
    cancel_client(Recv),
    {next_state, semi_duplex, Data#data{recv=Send}};
full_duplex(cast, {close, Ref, Reason},
            #data{recv=#client{ref=Ref} = Recv, send=Send} = Data) ->
    closed_send(Send),
    cancel_client(Recv),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next(Reason)};
full_duplex(info, {timeout, TRef, {Pid, Timeout}},
            #data{send=#client{timer=TRef} = Send, recv=Recv} = Data)
  when is_reference(TRef) ->
    closed_send(Send#client{timer=infinity}),
    cancel_client(Recv),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({timeout, Pid, Timeout})};
full_duplex(info, {timeout, TRef, {Pid, Timeout}},
            #data{recv=#client{timer=TRef}=Recv, send=Send} = Data)
  when is_reference(TRef) ->
    closed_send(Send),
    cancel_client(Send),
    cancel_client(Recv#client{timer=infinity}),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({timeout, Pid, Timeout})};
full_duplex(info, {'DOWN', MRef, _, Pid, Reason},
            #data{send=#client{mon=MRef}=Send, recv=Recv} = Data) ->
    cancel_client(Send#client{mon='DOWN'}),
    cancel_client(Recv),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({'DOWN', Pid, Reason})};
full_duplex(info, {'DOWN', MRef, _, Pid, Reason},
            #data{recv=#client{mon=MRef}=Recv, send=Send} = Data) ->
    closed_send(Send),
    cancel_client(Recv#client{mon='DOWN'}),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({'DOWN', Pid, Reason})};
full_duplex(Type, Event, Data) ->
    handle_event(Type, Event, full_duplex, Data).

closing(internal, {close, Reason},
        #data{recv=undefined, mod=Mod, socket=Socket} = Data) ->
    _ = try
            Mod:close(Reason, Socket)
        catch
            throw:_ ->
                ok
        end,
    NData = cancel_send(Data#data{socket=undefined}),
    {next_state, backoff, NData, connect_next()}.

code_change(_, State, Data, _) ->
    {state_functions, State, Data}.

terminate(_, closing, _) ->
    ok;
terminate(_, backoff, _) ->
    ok;
terminate(Reason, _, #data{mod=Mod, socket=Socket}) ->
    Mod:close(Reason, Socket).

%% Internal

send(Mod, Req, Socket, Conn, Ref) ->
    try Mod:send(Req, Socket) of
        Result ->
            handle_send(Result, Mod, Socket, Conn, Ref)
    catch
        throw:Result ->
            handle_send(Result, Mod, Socket, Conn, Ref);
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            send_exception(Class, Reason, Stack, Conn, Ref)
    end.

handle_send(Result, Mod, Socket, Conn, Ref) ->
    MRef = monitor(process, Conn),
    receive
        {?MODULE, Ref, Status} ->
            demonitor(MRef, [flush]),
            handle_send(Result, Status, Mod, Socket, Conn, Ref);
        {'DOWN', MRef, _, Pid, Reason} ->
            handle_send(Result, {'DOWN', Pid, Reason}, Mod, Socket, Conn, Ref)
    end.

handle_send({Next, NReq}, Status, Mod, Socket, Conn, Ref)
  when Next == send_recv; Next == recv; Next == result ->
    case Status of
        {'DOWN', _, _} = Down ->
            {error, Down};
        {Mode, Buffer, Broker} ->
            handle(Next, Mod, NReq, Buffer, Socket, Mode, Conn, Ref, Broker);
        closed ->
            {error, closed}
    end;
handle_send({close, Reason}, {Mode, _, _}, _, _, Conn, Ref)
  when Mode == half_duplex; Mode == full_duplex ->
    close(Conn, Ref, Reason),
    {error, Reason};
handle_send({closed, Reason}, _, _, _, _, _) ->
    {error, Reason};
handle_send(Other, _, _, _, Conn, Ref) ->
    Reason = {bad_return_value, Other},
    try
        exit(Reason)
    catch
        exit:Reason ->
            Stack = erlang:get_stacktrace(),
            send_exception(exit, Reason, Stack, Conn, Ref)
    end.

send_exception(Class, Reason, Stack, Conn, Ref) ->
    MRef = monitor(process, Conn),
    receive
        {?MODULE, Ref, {_, _, _}} ->
            demonitor(MRef, [flush]),
            exception(Conn, Ref, Class, Reason, Stack),
            erlang:raise(Class, Reason, Stack);
        {?MODULE, Ref, closed} ->
            demonitor(MRef, [flush]),
            erlang:raise(Class, Reason, Stack);
        {'DOWN', MRef, _, _, _} ->
            erlang:raise(Class, Reason, Stack)
    end.

handle(send_recv, Mod, Req, Buffer, Socket, Mode, Conn, Ref, Broker) ->
    try Mod:send_recv(Req, Buffer, Socket) of
        Result ->
            handle_send_recv(Result, Mod, Socket, Mode, Conn, Ref, Broker)
    catch
        throw:Result ->
            handle_send_recv(Result, Mod, Socket, Mode, Conn, Ref, Broker);
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            exception(Conn, Ref, Class, Reason, Stack),
            erlang:raise(Class, Reason, Stack)
    end;
handle(Other, Mod, Req, Buffer, Socket, full_duplex, Conn, Ref, Broker) ->
    Info = {Mod, Socket, Conn},
    To = {Conn, Ref},
    {await, Ref, Broker} = sbroker:async_ask_r(Broker, Info, To),
    handle(Other, Mod, Req, Buffer, Socket, Conn, Ref);
handle(Other, Mod, Req, Buffer, Socket, half_duplex, Conn, Ref, _) ->
    handle(Other, Mod, Req, Buffer, Socket, Conn, Ref).

handle(recv, Mod, Req, Buffer, Socket, Conn, Ref) ->
    try Mod:recv(Req, Buffer, Socket) of
        Result ->
            handle_recv(Result, Mod, Socket, Conn, Ref)
    catch
        throw:Result ->
            handle_recv(Result, Mod, Socket, Conn, Ref);
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            exception(Conn, Ref, Class, Reason, Stack),
            erlang:raise(Class, Reason, Stack)
    end;
handle(result, _, Result, Buffer, _, Conn, Ref) ->
    done(Conn, Ref, Buffer),
    {ok, Result};
handle(close, _, Reason, _, _, Conn, Ref) ->
    close(Conn, Ref, Reason),
    {error, Reason}.

handle_send_recv({Next, Req, Buffer}, Mod, Socket, Mode, Conn, Ref, Broker)
  when Next == recv; Next == result ->
    handle(Next, Mod, Req, Buffer, Socket, Mode, Conn, Ref, Broker);
handle_send_recv({close, Reason}, Mod, Socket, _, Conn, Ref, _) ->
    handle(close, Mod, Reason, undefined, Socket, Conn, Ref);
handle_send_recv(Other, _, _, _, Conn, Ref, _) ->
    Reason = {bad_return_value, Other},
    try
        exit(Reason)
    catch
        exit:Reason ->
            Stack = erlang:get_stacktrace(),
            exception(Conn, Ref, exit, Reason, Stack),
            erlang:raise(exit, Reason, Stack)
    end.

handle_recv({result, Req, Buffer}, Mod, Socket, Conn, Ref) ->
    handle(result, Mod, Req, Buffer, Socket, Conn, Ref);
handle_recv({close, Reason}, Mod, Socket, Conn, Ref) ->
    handle(close, Mod, Reason, undefined, Socket, Conn, Ref);
handle_recv(Other, _, _, Conn, Ref) ->
    Reason = {bad_return_value, Other},
    try
        exit(Reason)
    catch
        exit:Reason ->
            Stack = erlang:get_stacktrace(),
            exception(Conn, Ref, exit, Reason, Stack),
            erlang:raise(exit, Reason, Stack)
    end.

done(Conn, Ref, Buffer) ->
    gen_statem:cast(Conn, {done, Ref, Buffer}).

close(Conn, Ref, Reason) ->
    gen_statem:cast(Conn, {close, Ref, Reason}).

exception(Conn, Ref, Class, Reason, Stack) ->
    gen_statem:cast(Conn, {exception, Ref, Class, Reason, Stack}).

handle_init({Mode, Info, Broker}, Mod, Args)
  when Mode == half_duplex; Mode == full_duplex ->
    NBroker = lookup(Broker),
    MRef = monitor(process, NBroker),
    Data = #data{mod=Mod, args=Args, mode=Mode, broker=NBroker,
                 broker_mon=MRef, info=Info},
    {state_functions, backoff, Data, connect_next()};
handle_init({stop, _} = Stop, _, _) ->
    Stop;
handle_init(ignore, _, _) ->
    ignore;
handle_init(Other, _, _) ->
    {stop, {bad_return_value, Other}}.

lookup(Name) when is_atom(Name) ->
    whereis(Name);
lookup(Pid) when is_pid(Pid) ->
    Pid;
lookup({Name, Node}) when is_atom(Name), Node == node() ->
    whereis(Name);
lookup({Name, Node} = Process) when is_atom(Name), is_atom(Node) ->
    Process;
lookup({global, Name}) ->
    global:whereis_name(Name);
lookup({via, Mod, Name}) ->
    Mod:whereis_name(Name).

connect_next() ->
    {next_event, internal, connect}.

close_next(Reason) ->
    {next_event, internal, {close, Reason}}.

handle_connect({ok, Buffer, Socket}, Data) ->
    NData = ask_r(Data#data{socket=Socket, buffer=Buffer}),
    {next_state, await, NData};
handle_connect(Other, _) ->
    {stop, {bad_return_value, Other}}.

ask_r(#data{send=undefined, mod=Mod, socket=Socket, broker=Broker,
            broker_ref=BRef} = Data) ->
    Conn = self(),
    Info = {Mod, Socket, Conn},
    To = {Conn, BRef},
    {await, BRef, Broker} = sbroker:async_ask_r(Broker, Info, To),
    Data.

handle_event(info, {Ref, {go, _,  {Pid, _}, _, _}}, State, Data)
  when is_reference(Ref) ->
    closed_send(Ref, Pid),
    {next_state, State, Data};
handle_event(info, {Ref, {drop, _}}, State, Data) when is_reference(Ref) ->
    {next_state, State, Data};
handle_event(cast, {exception, Ref, Class, Reason, Stack}, _,
             #data{send=#client{ref=Ref}}) ->
    {stop, {Class, Reason, Stack}};
handle_event(cast, {exception, Ref, Class, Reason, Stack}, _,
             #data{recv=#client{ref=Ref}}) ->
    {stop, {Class, Reason, Stack}}.

start_client(Ref, Pid, Timeout, SojournTime) ->
    MRef = monitor(process, Pid),
    TRef = start_timer(Pid, Timeout, SojournTime),
    #client{ref=Ref, pid=Pid, mon=MRef, timer=TRef}.

cancel_client(#client{mon='DOWN', timer=TRef}) ->
    cancel_timer(TRef);
cancel_client(#client{mon=MRef, timer=TRef}) ->
    demonitor(MRef, [flush]),
    cancel_timer(TRef).

start_timer(_, infinity, _) ->
    infinity;
start_timer(Pid, Timeout, SojournTime) ->
    SoFar = erlang:convert_time_unit(SojournTime, native, milli_seconds),
    RemTimeout = max(0, Timeout-SoFar),
    erlang:start_timer(RemTimeout, self(), {Pid, Timeout}).

cancel_timer(infinity) ->
    ok;
cancel_timer(TRef) ->
    case erlang:cancel_timer(TRef) of
        false ->
            flush_timer(TRef);
        _ ->
            ok
    end.

flush_timer(TRef) ->
    receive
        {timeout, TRef, _} ->
            ok
    after
        0 ->
            error(badtimer, [TRef])
    end.

cancel_send(#data{send=unknown, broker=Broker, broker_ref=BRef} = Data) ->
    _ = sbroker:cancel(Broker, BRef, infinity),
    Data#data{send=undefined, broker_ref=make_ref()};
cancel_send(#data{send=undefined} = Data) ->
    Data#data{broker_ref=make_ref()}.

closed_send(#client{ref=Ref, pid=Pid} = Send) ->
    closed_send(Ref, Pid),
    cancel_client(Send).

closed_send(Ref, Pid) ->
    _ = Pid ! {?MODULE, Ref, closed},
    ok.

continue(#client{ref=Ref, pid=Pid}, Mode, Broker, Buffer) ->
    continue(Pid, Ref, Mode, Broker, Buffer).

continue(Pid, Ref, Mode, Broker, Buffer) ->
    _ = Pid ! {?MODULE, Ref, {Mode, Buffer, Broker}},
    ok.
