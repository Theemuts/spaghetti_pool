defmodule SpaghettiPool.App do
  @moduledoc false
  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false
    opts = [strategy: :one_for_one, name: SpaghettiPool]
    Supervisor.start_link([], opts)
  end
end