defmodule SpaghettiPool.ETS do
  @moduledoc false

  def create_monitors_table, do: :ets.new(:monitors, [:private])

  def insert(table, value), do: :ets.insert(table, value)

  def lookup_and_demonitor(%{monitors: mons, workers: w} = state_data, pid, key \\ nil) do
    case :ets.lookup(mons, pid) do
      [{nil, _, m_ref, key}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, pid)
        {key, state_data}
      [{^pid, _, m_ref, key}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, pid)
        {key, %{state_data | workers: [pid|w]}}
      [] ->
        {key, %{state_data | workers: [pid|w]}}
    end
  end

  def lookup_and_demonitor_worker(%{monitors: mons, workers: w} = state_data, pid) do
    with [{pid, _, m_ref, key}] <- :ets.lookup(mons, pid) do
      true = Process.demonitor(m_ref)
      true = :ets.delete(mons, pid)
      {:ok, state_data, key}
    else
      _ -> {:error, :new_worker, Enum.member?(w, pid)}
    end
  end

  def match_and_demonitor(%{monitors: mons, processing_queue: q} = state_data, c_ref, key \\ nil) do
    with [[pid, m_ref]] <- :ets.match(mons, {:"$1", c_ref, :"$2", key}) do
      Process.demonitor(m_ref, [:flush])
      true = :ets.delete(mons, pid)
      {:ok, state_data, pid}
    else
      _ ->
        cancel = fn
          ({_, ^c_ref, m_ref, _}) ->
            not Process.demonitor(m_ref, [:flush])
          (_) ->
            true
        end
        {:error, %{state_data | processing_queue: q = :queue.filter(cancel, q)}}
    end
  end

  def match_and_demonitor_lock(%{monitors: mons} = state_data, l_ref) do
    with [[m_ref]] <- :ets.match(mons, {nil, l_ref, :"$1", :lock}) do
      Process.demonitor(m_ref, [:flush])
      :ets.delete(mons, nil)
    end

    %{state_data | locked_by: nil}
  end

  def match_down(%{monitors: mons} = state_date, m_ref) do
    case :ets.match(mons, {:"$1", :"_", m_ref, :"$2"}) do
      [x] -> x
      [] -> nil
    end
  end
end