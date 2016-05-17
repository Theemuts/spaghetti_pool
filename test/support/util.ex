defmodule SpaghettiPool.Support.Util do
  @moduledoc false

  def name do
    :crypto.rand_bytes(43) |> Base.encode64 |> String.to_atom
  end
end