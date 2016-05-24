defmodule SpaghettiPool.ETS do
  @moduledoc false

  def create_monitors_table, do: :ets.new(:monitors, [:private])

  def match_and_demonitor(%{monitors: mons, processing_queue: q} = state_data, c_ref, key \\ nil) do
    case :ets.match(mons, {:"$1", c_ref, :"$2", key}) do
      [[pid, m_ref]] ->
        Process.demonitor(m_ref, [:flush])
        true = :ets.delete(mons, pid)
        {:ok, state_data, pid}
      [] ->
        cancel = fn
          ({_, ref, m_ref, _}) when ref == c_ref ->
            Process.demonitor(m_ref, [:flush])
            false
          (_) ->
            true
        end

        {:error, %{state_data | processing_queue: q = :queue.filter(cancel, q)}}
    end
  end
  
end