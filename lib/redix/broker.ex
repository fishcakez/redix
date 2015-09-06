defmodule Redix.Broker do
  @moduledoc false

  def start_link() do
    :sbroker.start_link({:local, __MODULE__}, __MODULE__, nil, [time_unit: :micro_seconds])
  end

  def init(_) do
    client_queue = Application.get_env(:redix, :broker_client_queue)
    worker_queue = Application.get_env(:redix, :broker_worker_queue)
    timeout      = Application.get_env(:redix, :broker_timeout)
    {:ok, {client_queue, worker_queue, timeout}}
  end
end
