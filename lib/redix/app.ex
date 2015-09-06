defmodule Redix.App do
  @moduledoc false
  use Application

  def start(_, _) do
    import Supervisor.Spec
    broker = worker(Redix.Broker, [])
    Supervisor.start_link([broker], [strategy: :one_for_one])
  end
end
