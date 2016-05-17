defmodule SpaghettiPool.Support.Worker do
  @moduledoc false

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def init(x), do: {:ok, x}
end