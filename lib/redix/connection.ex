defmodule Redix.Connection do
  @moduledoc false

  use Connection

  alias Redix.Connection.Auth
  require Logger

  def tcp_error(conn, ref, reason), do: Connection.cast(conn, {:tcp_error, ref, reason})

  def client_error(conn, ref), do: Connection.cast(conn, {:client_error, ref})

  def done(conn, ref), do: Connection.cast(conn, {:done, ref})

  @type state :: %{}

  @initial_state %{
    socket: nil,
    opts: nil,
    reconnection_attempts: 0,
    broker_monitor: nil,
    client_ref: nil,
    client_monitor: nil,
    client_timer: nil
  }

  @default_timeout 5000
  @default_broker Redix.Broker

  @socket_opts [:binary, active: false]

  ## Callbacks

  @doc false
  def init(s) do
    {:connect, :init, Dict.merge(@initial_state, s)}
  end

  @doc false
  def connect(info, s)

  def connect(info, %{opts: opts} = s) do
    {host, port, socket_opts, timeout} = tcp_connection_opts(opts)

    case :gen_tcp.connect(host, port, socket_opts, timeout) do
      {:ok, socket} ->
        setup_socket_buffers(socket)
        handshake(%{s | socket: socket, reconnection_attempts: 0})
      {:error, reason} ->
        Logger.error "Error connecting to Redis (#{host_for_logging(s)}): #{inspect reason}"
        handle_connection_error(s, info, reason)
    end
  end

  @doc false
  def disconnect(reason, s)

  def disconnect(reason, %{socket: socket} = s) do
    if socket, do: :gen_tcp.close(socket)
    backoff_or_stop(%{s | socket: nil}, 0, {:shutdown, reason})
  end

  @doc false
  def handle_cast(operation, s)

  def handle_cast({:done, ref}, %{client_ref: ref} = s)
  when is_reference(ref) do
    {:noreply, s |> client_done() |> broker_ask()}
  end

  def handle_cast({:tcp_error, ref, reason}, %{client_ref: ref} = s)
  when is_reference(ref) do
    Logger.error "Disconnected from Redis (#{host_for_logging(s)}): #{inspect reason}"
    {:disconnect, {:tcp_error, reason}, client_done(s)}
  end

  def handle_cast({:client, ref}, %{client_ref: ref} = s)
  when is_reference(ref) do
    {:disconnect, :client_error, client_done(s)}
  end

  def handle_cast(:stop, s) do
    {:stop, :normal, s}
  end

  def handle_cast(msg, %{client_ref: ref} = s) when elem(msg, 1) != ref do
    {:noreply, s}
  end

  @doc false
  def handle_info(msg, s)

  def handle_info({mon, msg}, %{broker_monitor: mon} = s) when is_reference(mon) do
    handle_broker_msg(msg, s)
  end

  def handle_info({:DOWN, mon, _, _, reason},
                  %{broker_monitor: mon} = s) when is_reference(mon) do
    {:stop, {:shutdown, {:broker_terminated, reason}}, s}
  end

  def handle_info({:DOWN, mon, _, _, reason},
                  %{client_monitor: mon} = s) when is_reference(mon) do
    {:disconnect, {:client_terminated, reason}, client_done(%{s | client_monitor: nil})}
  end

  def handle_info({:timeout, timer, ref}, %{client_timer: timer, client_ref: ref} = s)
  when is_reference(timer) do
    {:disconnect, :client_timeout, client_done(%{s | client_timer: nil})}
  end

  def handle_info(_, s) do
    {:noreply, s}
  end

  ## Helper functions

  defp handshake(s) do
    case Auth.auth_and_select_db(s) do
      {:ok, s}             -> {:ok, broker_ask(s)}
      {:stop, _, _} = stop -> stop
    end
  end

  defp broker_ask(%{client_ref: nil, broker_monitor: nil, opts: opts,
                    socket: socket} = s) do
    broker = opts[:broker] || @default_broker
    {:await, mon, _} = :sbroker.async_ask_r(broker, {self(), socket})
    %{s | broker_monitor: mon}
  end

  defp handle_broker_msg({:go, ref, {client, timeout}, _, _},
                         %{client_ref: nil, broker_monitor: mon} = s) do
    client_mon = Process.monitor(client)
    if timeout != :infinity, do: timer = :erlang.start_timer(timeout, self(), ref)
    Process.demonitor(mon, [:flush])
    s = %{s | client_monitor: client_mon, client_ref: ref, client_timer: timer,
              broker_monitor: nil}
    {:noreply, s}
  end

  defp handle_broker_msg({:drop, _},
                         %{client_ref: nil, broker_monitor: mon} = s) do
    Process.demonitor(mon, [:flush])
    {:disconnect, :broker_drop, %{s | broker_monitor: nil}}
  end

  defp client_done(%{client_monitor: mon, client_timer: timer} = s) do
    if mon, do: Process.demonitor(mon, [:flush])
    if timer, do: cancel_timer(timer)
    %{s | client_ref: nil, client_monitor: nil, client_timer: nil}
  end

  defp cancel_timer(timer) do
    unless :erlang.cancel_timer(timer) do
      receive do
        {:timeout, ^timer, _} -> :ok
      after
        0 -> raise "timer #{inspect timer} does not exist"
      end
    end
  end

  # Extracts the TCP connection options (host, port and socket opts) from the
  # given `opts`.
  defp tcp_connection_opts(opts) do
    host = to_char_list(Keyword.fetch!(opts, :host))
    port = Keyword.fetch!(opts, :port)
    socket_opts = @socket_opts ++ Keyword.fetch!(opts, :socket_opts)
    timeout = opts[:timeout] || @default_timeout

    {host, port, socket_opts, timeout}
  end

  # Setups the `:buffer` option of the given socket.
  defp setup_socket_buffers(socket) do
    {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
      :inet.getopts(socket, [:sndbuf, :recbuf, :buffer])

    buffer = buffer |> max(sndbuf) |> max(recbuf)
    :ok = :inet.setopts(socket, [buffer: buffer])
  end

  defp host_for_logging(%{opts: opts} = _s) do
    "#{opts[:host]}:#{opts[:port]}"
  end

  # If `info` is :backoff then this is a *reconnection* attempt, so if there's
  # an error let's try to just reconnect after a backoff time (if we're under
  # the max number of retries). If `info` is :init, then this is the first
  # connection attempt so if it fails let's just die.
  defp handle_connection_error(s, :init, reason),
    do: {:stop, reason, s}
  defp handle_connection_error(s, :backoff, reason),
    do: backoff_or_stop(s, s.opts[:backoff], reason)

  # This function is called every time we want to try and reconnect. It returns
  # {:backoff, ...} if we're below the max number of allowed reconnection
  # attempts (or if there's no such limit), {:stop, ...} otherwise.
  defp backoff_or_stop(s, backoff, stop_reason) do
    s = update_in(s.reconnection_attempts, &(&1 + 1))

    if attempt_to_reconnect?(s) do
      {:backoff, backoff, s}
    else
      {:stop, stop_reason, s}
    end
  end

  defp attempt_to_reconnect?(%{opts: opts, reconnection_attempts: attempts}) do
    max_attempts = opts[:max_reconnection_attempts]
    is_nil(max_attempts) or (max_attempts > 0 and attempts <= max_attempts)
  end
end
