defmodule SpaghettiPool.ETS do
  @moduledoc false

  @doc false
  @spec create_monitors_table :: :ets.tid
  def create_monitors_table, do: :ets.new(:monitors, [:private])

  @doc false
  @spec insert(:ets.tid, tuple) :: true
  def insert(table, value), do: :ets.insert(table, value)

  @doc false
  @spec lookup_and_demonitor(SpaghettiPool.state, pid | nil, SpaghettiPool.key | :lock) :: {SpaghettiPool.key, SpaghettiPool.state} | {:lock, SpaghettiPool.state}
  def lookup_and_demonitor(%{monitors: mons, workers: w, strategy: s} = state_data, pid, key \\ nil) do
    case :ets.lookup(mons, pid) do
      [{nil, c_ref, m_ref, key}] ->
        true = Process.demonitor(m_ref)
        true = Process.demonitor(c_ref)
        true = :ets.delete(mons, pid)
        {key, state_data}
      [{^pid, c_ref, m_ref, key}] ->
        true = Process.demonitor(m_ref)
        true = Process.demonitor(c_ref)
        true = :ets.delete(mons, pid)
        w = if s == :lifo, do: [pid | w], else: w ++ [pid]
        {key, %{state_data | workers: w}}
      [] ->
        w = if s == :lifo, do: [pid | w], else: w ++ [pid]
        {key, %{state_data | workers: w}}
    end
  end

  @doc false
  @spec lookup_and_demonitor_worker(SpaghettiPool.state, pid) :: {:ok, SpaghettiPool.state, SpaghettiPool.key} | {:error, :new_worker, boolean}
  def lookup_and_demonitor_worker(%{monitors: mons, workers: w} = state_data, pid) do
    case :ets.lookup(mons, pid) do
      [{pid, _, m_ref, key}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, pid)
        {:ok, state_data, key}
      _ ->
        {:error, :new_worker, Enum.member?(w, pid)}
    end
  end

  @doc false
  @spec match_and_demonitor(SpaghettiPool.state, reference, SpaghettiPool.key) :: {:ok, SpaghettiPool.state, pid} | {:error, SpaghettiPool.state}
  def match_and_demonitor(%{monitors: mons, processing_queue: q} = state_data, c_ref, key \\ nil) do
    case :ets.match(mons, {:"$1", c_ref, :"$2", key}) do
      [[pid, m_ref]] ->
        Process.demonitor(m_ref, [:flush])
        true = :ets.delete(mons, pid)
        {:ok, state_data, pid}
      _ ->
        cancel = fn
          ({_, ^c_ref, m_ref, _}) ->
            Process.demonitor(m_ref, [:flush]); false
          (_) ->
            true
        end
        {:error, %{state_data | processing_queue: q = :queue.filter(cancel, q)}}
    end
  end

  @doc false
  @spec match_and_demonitor_lock(SpaghettiPool.state, reference) :: SpaghettiPool.state
  def match_and_demonitor_lock(%{monitors: mons} = state_data, l_ref) do
    with [[m_ref]] <- :ets.match(mons, {nil, l_ref, :"$1", :lock}) do
      Process.demonitor(m_ref, [:flush])
      :ets.delete(mons, nil)
    end

    %{state_data | locked_by: nil}
  end

  @doc false
  @spec match_down(SpaghettiPool.state, reference) :: List.t | nil
  def match_down(%{monitors: mons}, m_ref) do
    case :ets.match(mons, {:"$1", :"_", m_ref, :"$2"}) do
      [x] -> x
      [] -> nil
    end
  end
end