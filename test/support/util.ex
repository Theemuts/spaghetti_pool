defmodule SpaghettiPool.Support.Util do
  @moduledoc false

  alias SpaghettiPool.Support.Request

  def name do
    :crypto.rand_bytes(45) |> Base.encode64 |> String.to_atom
  end

  def workers(n) when n > 0 do
    1..n
    |> Enum.map(fn(_) ->
         {:ok, r} = Request.start
         r
       end)
  end

  def teardown(sup, reqs \\ [])
  def teardown(sup, req) when is_pid(req) do
    GenServer.stop(req)
    Supervisor.stop(sup)
  end

  def teardown(sup, []) do
    Supervisor.stop(sup)
  end

  def teardown(sup, reqs) do
    for req <- reqs, do: GenServer.stop(req)
    Supervisor.stop(sup)
  end
end