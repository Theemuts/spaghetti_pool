defmodule SpaghettiPoolSupervisor do
  @moduledoc false
  use Supervisor

  def start_link(mod, args) do
    Supervisor.start_link(__MODULE__, {mod, args})
  end

  def init({mod, args}) do
    {:ok, {{:simple_one_for_one, 0, 1},
          [{mod, {mod, :start_link, [args]}, :temporary, 5000, :worker, [mod]}]}}
  end
end