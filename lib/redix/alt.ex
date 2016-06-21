defmodule Redix.Alt do
  @moduledoc """
  This module provides the main API to interface with Redis.

  ## Overview

  `start_link/2` starts a process that connects to Redis. Each Elixir process
  started with this function maps to a client TCP connection to the specified
  Redis server.

  The architecture is very simple: when you issue commands to Redis (via
  `command/3` or `pipeline/3`), the Redix process sends the command to Redis right
  away and is immediately able to send new commands. When a response arrives
  from Redis, only then the Redix process replies to the caller with the
  response. This pattern avoids blocking the Redix process for each request (until
  a response arrives), increasing the performance of this driver.

  ## Reconnections

  Redix tries to be as resilient as possible: it tries to recover automatically
  from most network errors.

  If there's a network error when sending data to Redis or if the connection to Redis
  drops, Redix tries to reconnect. The first reconnection attempt will happen
  after a fixed time interval; if this attempt fails, reconnections are
  attempted until successful, and the time interval between reconnections is
  increased exponentially. Some aspects of this behaviour can be configured; see
  `start_link/2` and the "Reconnections" page in the docs for more information.

  All this behaviour is implemented using the
  [connection](https://github.com/fishcakez/connection) library (a dependency of
  Redix).
  """

  @type command :: [binary]

  @default_timeout 5000

  @doc """
  Starts a connection to Redis.

  This function returns `{:ok, pid}` if the Redix process is started
  successfully.

  The actual TCP connection to the Redis server may happen either synchronously,
  before `start_link/2` returns, or asynchronously: this behaviour is decided by
  the `:sync_connect` option (see below).

  This function accepts two arguments: the options to connect to the Redis
  server (like host, port, and so on) and the options to manage the connection
  and the resiliency. The Redis options can be specified as a keyword list or as
  a URI.

  ## Redis options

  ### URI

  In case `uri_or_redis_opts` is a Redis URI, it must be in the form:

      redis://[:password@]host[:port][/db]

  Here are some examples of valid URIs:

      redis://localhost
      redis://:secret@localhost:6397
      redis://example.com:6380/1

  Usernames before the password are ignored, so the these two URIs are
  equivalent:

      redis://:secret@localhost
      redis://myuser:secret@localhost

  The only mandatory thing when using URIs is the host. All other elements
  (password, port, database) are optional and their default value can be found
  in the "Options" section below.

  ### Options

  The following options can be used to specify the parameters used to connect to
  Redis (instead of a URI as described above):

    * `:host` - (string) the host where the Redis server is running. Defaults to
      `"localhost"`.
    * `:port` - (integer) the port on which the Redis server is
      running. Defaults to `6379`.
    * `:password` - (string) the password used to connect to Redis. Defaults to
      `nil`, meaning no password is used. When this option is provided, all Redix
      does is issue an `AUTH` command to Redis in order to authenticate.
    * `:database` - (integer or string) the database to connect to. Defaults to
      `nil`, meaning don't connect to any database (Redis connects to database
      `0` by default). When this option is provided, all Redix does is issue a
      `SELECT` command to Redis in order to select the given database.

  ## Connection options

  `connection_opts` is a list of options used to manage the connection. These
  are the Redix-specific options that can be used:

    * `:socket_opts` - (list of options) this option specifies a list of options
      that are passed to `:gen_tcp.connect/4` when connecting to the Redis
      server. Some socket options (like `:active` or `:binary`) will be
      overridden by Redix so that it functions properly. Defaults to `[]`.
    * `:sync_connect` - (boolean) decides whether Redix should initiate the TCP
      connection to the Redis server *before* or *after* returning from
      `start_link/2`. This option also changes some reconnection semantics; read
      the "Reconnections" page in the docs.
    * `:backoff_initial` - (integer) the initial backoff time (in milliseconds),
      which is the time that will be waited by the Redix process before
      attempting to reconnect to Redis after a disconnection or failed first
      connection. See the "Reconnections" page in the docs for more information.
    * `:backoff_max` - (integer) the maximum length (in milliseconds) of the
      time interval used between reconnection attempts. See the "Reconnections"
      page in the docs for more information.

  In addition to these options, all options accepted by
  `Connection.start_link/3` (and thus `GenServer.start_link/3`) are forwarded to
  it. For example, a Redix connection can be registered with a name by using the
  `:name` option:

      Redix.start_link([], name: :redix)
      Process.whereis(:redix)
      #=> #PID<...>

  ## Examples

      iex> Redix.start_link()
      {:ok, #PID<...>}

      iex> Redix.start_link(host: "example.com", port: 9999, password: "secret")
      {:ok, #PID<...>}

      iex> Redix.start_link([database: 3], [name: :redix_3])
      {:ok, #PID<...>}

  """
  @spec start_link(binary | Keyword.t, Keyword.t) :: GenServer.on_start
  def start_link(uri_or_redis_opts \\ [], connection_opts \\ [])

  def start_link(uri, other_opts) when is_binary(uri) and is_list(other_opts) do
    uri |> Redix.URI.opts_from_uri() |> start_link(other_opts)
  end

  def start_link(redis_opts, other_opts) do
    {redix_opts, connection_opts} = Redix.Utils.sanitize_starting_opts(redis_opts, other_opts)
    :duplex_connection.start_link(__MODULE__, {Redix.Broker, redix_opts}, connection_opts)
  end

  @doc """
  Issues a pipeline of commands on the Redis server.

  `commands` must be a list of commands, where each command is a list of strings
  making up the command and its arguments. The commands will be sent as a single
  "block" to Redis, and a list of ordered responses (one for each command) will
  be returned.

  The return value is `{:ok, results}` if the request is successful, `{:error,
  reason}` otherwise.

  Note that `{:ok, results}` is returned even if `results` contains one or more
  Redis errors (`Redix.Error` structs). This is done to avoid having to walk the
  list of results (a `O(n)` operation) to look for errors, leaving the
  responsibility to the user. That said, errors other than Redis errors (like
  network errors) always cause the return value to be `{:error, reason}`.

  If `commands` is an empty list (`[]`), then a `Redix.ConnectionError` will be
  raised right away. If any of the commands in `commands` is an empty command
  (`[]`), `{:error, :empty_command}` will be returned (which mirrors the
  behaviour of `command/3` in case of empty commands).

  ## Options

    * `:timeout` - (integer or `:infinity`) request timeout (in
      milliseconds). Defaults to `#{@default_timeout}`.

  ## Examples

      iex> Redix.pipeline(conn, [~w(INCR mykey), ~w(INCR mykey), ~w(DECR mykey)])
      {:ok, [1, 2, 1]}

      iex> Redix.pipeline(conn, [~w(SET k foo), ~w(INCR k), ~(GET k)])
      {:ok, ["OK", %Redix.Error{message: "ERR value is not an integer or out of range"}, "foo"]}

  If Redis goes down (before a reconnection happens):

      iex> Redix.pipeline(conn, [~w(SET mykey foo), ~w(GET mykey)])
      {:error, :closed}

  """
  @spec pipeline(GenServer.server, [command], Keyword.t) ::
    {:ok, [Redix.Protocol.redis_value]} |
    {:error, atom}
  def pipeline(conn \\ Redix.Broker, commands, opts \\ [])

  def pipeline(_conn, [], _opts) do
    raise(Redix.ConnectionError, "no commands passed to the pipeline")
  end

  def pipeline(conn, commands, opts) do
    try do
      Enum.each(commands, fn
        [] ->
          throw(:empty_command)
        [command | _] when command in ~w(SUBSCRIBE PSUBSCRIBE UNSUBSCRIBE PUNSUBSCRIBE) ->
          throw({:pubsub_command, command})
        _ ->
          :ok
      end)
      do_pipeline(conn, commands, opts[:timeout] || @default_timeout)
    catch
      :throw, error -> {:error, error}
    end
  end

  @doc """
  Issues a pipeline of commands to the Redis server, raising if there's an error.

  This function works similarly to `pipeline/3`, except:

    * if there are no errors in issuing the commands (even if there are one or
      more Redis errors in the results), the results are returned directly (not
      wrapped in a `{:ok, results}` tuple).
    * if there's a connection error then a `Redix.ConnectionError` exception is raised.

  For more information on why nothing is raised if there are one or more Redis
  errors (`Redix.Error` structs) in the list of results, look at the
  documentation for `pipeline/3`.

  This function accepts the same options as `pipeline/3`.

  ## Options

    * `:timeout` - (integer or `:infinity`) request timeout (in
      milliseconds). Defaults to `#{@default_timeout}`.

  ## Examples

      iex> Redix.pipeline!(conn, [~w(INCR mykey), ~w(INCR mykey), ~w(DECR mykey)])
      [1, 2, 1]

      iex> Redix.pipeline!(conn, [~w(SET k foo), ~w(INCR k), ~(GET k)])
      ["OK", %Redix.Error{message: "ERR value is not an integer or out of range"}, "foo"]

  If Redis goes down (before a reconnection happens):

      iex> Redix.pipeline!(conn, [~w(SET mykey foo), ~w(GET mykey)])
      ** (Redix.ConnectionError) :closed

  """
  @spec pipeline!(GenServer.server, [command], Keyword.t) ::
    [Redix.Protocol.redis_value] | no_return
  def pipeline!(conn, commands,  opts \\ []) do
    case pipeline(conn, commands, opts) do
      {:ok, resp} -> resp
      {:error, error} -> raise Redix.ConnectionError, error
    end
  end

  @doc """
  Issues a command on the Redis server.

  This function sends `command` to the Redis server and returns the response
  returned by Redis. `pid` must be the pid of a Redix connection. `command` must
  be a list of strings making up the Redis command and its arguments.

  The return value is `{:ok, response}` if the request is successful and the
  response is not a Redis error. `{:error, reason}` is returned in case there's
  an error in sending the response or in case the response is a Redis error. In
  the latter case, `reason` will be the error returned by Redis.

  If the given command (`cmd`) is an empty command (`[]`), `{:error,
  :empty_command}` will be returned.

  ## Options

    * `:timeout` - (integer or `:infinity`) request timeout (in
      milliseconds). Defaults to `#{@default_timeout}`.

  ## Examples

      iex> Redix.command(conn, ["SET", "mykey", "foo"])
      {:ok, "OK"}
      iex> Redix.command(conn, ["GET", "mykey"])
      {:ok, "foo"}

      iex> Redix.command(conn, ["INCR", "mykey"])
      {:error, "ERR value is not an integer or out of range"}

  If Redis goes down (before a reconnection happens):

      iex> Redix.command(conn, ["GET", "mykey"])
      {:error, :closed}

  """
  @spec command(GenServer.server, command, Keyword.t) ::
    {:ok, Redix.Protocol.redis_value} |
    {:error, atom | Redix.Error.t}
  def command(conn, command, opts \\ []) do
    case pipeline(conn, [command], opts) do
      {:ok, [%Redix.Error{} = error]} ->
        {:error, error}
      {:ok, [resp]} ->
        {:ok, resp}
      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Issues a command on the Redis server, raising if there's an error.

  This function works exactly like `command/3` but:

    * if the command is successful, then the result is returned not wrapped in a
      `{:ok, result}` tuple.
    * if there's a Redis error, a `Redix.Error` error is raised (with the
      original message).
    * if there's a network error (e.g., `{:error, :closed}`) a `Redix.ConnectionError`
      error is raised.

  This function accepts the same options as `command/3`.

  ## Options

    * `:timeout` - (integer or `:infinity`) request timeout (in
      milliseconds). Defaults to `#{@default_timeout}`.

  ## Examples

      iex> Redix.command!(conn, ["SET", "mykey", "foo"])
      "OK"

      iex> Redix.command!(conn, ["INCR", "mykey"])
      ** (Redix.Error) ERR value is not an integer or out of range

  If Redis goes down (before a reconnection happens):

      iex> Redix.command!(conn, ["GET", "mykey"])
      ** (Redix.ConnectionError) :closed

  """
  @spec command!(GenServer.server, command, Keyword.t) ::
    Redix.Protocol.redis_value | no_return
  def command!(conn, command, opts \\ []) do
    case command(conn, command, opts) do
      {:ok, resp} ->
        resp
      {:error, %Redix.Error{} = error} ->
        raise error
      {:error, error} ->
        raise Redix.ConnectionError, error
    end
  end

  def init({broker, opts}) do
    backoff = :backoff.init(1000, 20000, self(), __MODULE__)
    {:full_duplex, opts, broker, backoff}
  end

  def open(opts) do
    case Redix.Utils.connect(opts) do
      {:ok, socket} ->
        :ok = :inet.setopts(socket, [active: false])
        receive do
          {:tcp, ^socket, buffer} ->
            {:ok, buffer, socket}
          {:tcp_error, ^socket, error} ->
            :gen_tcp.close(socket)
            {:error, error}
          {:tcp_closed, ^socket} ->
            :gen_tcp.close(socket)
            {:error, :closed}
        after
          0 ->
            {:ok, <<>>, socket}
        end
      {:error, _} = error ->
        error
      {:stop, reason} ->
        exit(reason)
    end
  end

  def activate(<<>>, socket) do
    case :inet.setopts(socket, [active: :once]) do
      :ok              -> {:active, :active}
      {:error, reason} -> {:close, reason}
    end
  end
  def activate(buffer, _) when is_binary(buffer) do
    {:passive, buffer}
  end

  def data({:tcp, socket, buffer}, :active, socket) do
    {:passive, buffer}
  end
  def data({:tcp_closed, socket}, :active, socket) do
    {:close, :closed}
  end
  def data({:tcp_error, socket, reason}, :active, socket) do
    {:close, reason}
  end
  def data(_, :active, _) do
    {:active, :active}
  end

  def pacify(:active, socket) do
    result = :inet.setopts(socket, [active: false])
    receive do
      {:tcp, ^socket, data} when result == :ok ->
        {:passive, data}
      {:tcp, ^socket, _} ->
        {:error, reason} = result
        {:close, reason}
      {:tcp_closed, ^socket} ->
        {:close, :closed}
      {:tcp_error, ^socket, reason} ->
        {:close, reason}
    after
      0  ->
        case result do
          :ok              -> {:passive, <<>>}
          {:error, reason} -> {:close, reason}
        end
    end
  end

  def send({data, len}, socket) do
    case :gen_tcp.send(socket, data) do
      :ok             -> {:recv, len}
      {:error, error} -> {:close, error}
    end
  end

  def send_recv(req, _, _) do
    raise "bad request: " <> inspect(req)
  end

  def recv(len, buffer, socket) do
    case Redix.Protocol.parse_multi(buffer, len) do
      {:ok, resp, buffer} ->
        {:result, resp, :binary.copy(buffer)}
      {:continuation, cont} ->
        recv_loop(:gen_tcp.recv(socket, 0, :infinity), cont, socket)
    end
  end

  defp recv_loop({:ok, data}, cont, socket) do
    case cont.(data) do
      {:ok, resp, buffer} ->
        {:result, resp, :binary.copy(buffer)}
      {:continuation, cont} ->
        recv_loop(:gen_tcp.recv(socket, 0, :infinity), cont, socket)
    end
  end
  defp recv_loop({:error, reason}, _, _) do
    {:close, reason}
  end

  def close(_, socket), do: :gen_tcp.close(socket)

  ## Internal

  defp do_pipeline(conn, commands, timeout) do
    pack = fn(command, {data, len}) ->
      {[data | Redix.Protocol.pack(command)], len+1}
    end
    {_data, _len} = req = Enum.reduce(commands, {[], 0}, pack)
    case :duplex_connection.send_recv(conn, req, timeout) do
      {:ok, _} = ok ->
        ok
      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end
end
