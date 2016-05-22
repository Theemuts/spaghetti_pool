defmodule SpaghettiPool.Support.Worker do
  @moduledoc false

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, [])
  end

  def init(_), do: {:ok, nil}
end