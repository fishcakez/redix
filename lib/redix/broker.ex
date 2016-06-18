defmodule Redix.Broker do

  @behaviour :sbroker

  def start_link() do
    :sbroker.start_link({:local, __MODULE__}, __MODULE__, nil, [])
  end

  def init(_) do
    ask   = {:sbroker_timeout_queue, {:out, 5_000, :drop, 0, :infinity}}
    ask_r = {:sbroker_drop_queue, {:out, :drop, :infinity}}
    {:ok, {ask, ask_r, []}}
  end
end
