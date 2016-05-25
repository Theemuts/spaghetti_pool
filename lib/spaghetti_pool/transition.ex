defmodule SpaghettiPool.Transition do
  @moduledoc false
  # This module handles all state transitions that the pool can undergo.

  @doc false
  #@spec transition(SpaghettiPool.state_name, SpaghettiPool.state, SpaghettiPool.key) :: SpaghettiPool.transition
  def transition(state_name, %{processing_queue: q, write_queue: wq, read_queue: rq, pending_write: pw} = state_data, key \\ nil) do
    empty_processing = :queue.is_empty(q)
    all_available = all_workers_available?(state_data)
    empty_read = :queue.is_empty(rq)
    empty_write = :queue.is_empty(wq)
    no_pending = map_size(pw) == 0

    transition(state_name, state_data, empty_processing, all_available, empty_read, empty_write, no_pending, pw[key], key)
  end

  #@spec all_workers_available?(SpaghettiPool.state) :: boolean
  defp all_workers_available?(%{supervisor: sup, workers: w}) do
    n_workers = sup |> Supervisor.which_children |> length
    n_avail = length(w)
    n_avail == n_workers
  end

  #@spec transition(SpaghettiPool.state_name, SpaghettiPool.state, boolean, boolean, boolean, boolean, boolean, :queue.queue | nil, SpaghettiPool.key) :: SpaghettiPool.transition
  defp transition(state_name, state_data, empty_processing, all_available, empty_read, empty_write, no_pending, pending_write, key)

  defp transition(:request_lock, state_data, _, _, _, _, _, _, _) do
    {:next_state, :pending_locked, state_data}
  end

  defp transition(:pending_locked, %{mode: :w} = state_data, _, _, _, _, _, pw, key) when not is_nil(pw) and pw != {[], []} and not is_nil(key) do
    SpaghettiPool.finish_writes({:handle_pending, key}, state_data)
  end

  defp transition(:pending_locked, state_data, _, true, _, _, _, _, _) do
    SpaghettiPool.locked(:all_workers_acquired, state_data)
  end

  defp transition(:pending_locked, state_data, _, _, _, _, _, _, _) do
    {:next_state, :pending_locked, state_data}
  end

  defp transition(:inform_lock_success, state_data, _, _, _, _, _, _, _) do
    {:reply, :ok, :locked, state_data}
  end

  defp transition(:inform_lock_fail, state_data, _, _, _, _, _, _, _) do
    {:reply, :error, :locked, state_data}
  end

  defp transition(:locked, state_data, _, _, _, _, _, _, _) do
    {:next_state, :locked, state_data}
  end

  # Processing queue empty, not all workers have returned yet
  defp transition(:await_readers, %{mode: :r} = state_data, true, false, _, _, _, _, _) do
    {:next_state, :await_readers, state_data}
  end

  # Processing queue empty, all workers have returned, write queue not empty.
  defp transition(:await_readers, %{write_queue: wq, mode: :r} = state_data, true, true, _, false, _, _, _) do
    SpaghettiPool.handle_writes(:handle_next, %{state_data | processing_queue: wq, write_queue: :queue.new, mode: :w})
  end

  # Processing queue empty, all workers have returned, write queue empty, read queue not empty.
  defp transition(:await_readers, %{read_queue: rq, mode: :r} = state_data, true, true, false, _, _, _, _) do
    SpaghettiPool.handle_reads(:handle_next, %{state_data | processing_queue: rq, read_queue: :queue.new, mode: :r})
  end

  # All workers returned, no work in any queue
  defp transition(:await_readers, %{mode: :r} = state_data, true, true, _, _, _, _, _) do
    {:next_state, :all_workers_available, state_data}
  end

  # Processing queue empty, not all workers have returned yet
  defp transition(:await_writers, state_data, true, false, _, _, _, _, _) do
    {:next_state, :await_writers, state_data}
  end

  # Processing queue empty, all workers have returned, read queue not empty.
  defp transition(:await_writers, %{read_queue: rq, mode: :w} = state_data, true, true, false, _, _, _, _) do
    SpaghettiPool.handle_reads(:handle_next, %{state_data | processing_queue: rq, read_queue: :queue.new, mode: :r})
  end

  # Processing queue empty, all workers have returned, read queue empty, write queue not empty.
  defp transition(:await_writers, %{write_queue: wq, mode: :w} = state_data, true, true, _, false, _, _, _) do
    SpaghettiPool.handle_writes(:handle_next, %{state_data | processing_queue: wq, write_queue: :queue.new, mode: :w})
  end

  # No work in any queue
  defp transition(:await_writers, %{mode: :w} = state_data, true, true, true, true, true, _, _) do
    {:next_state, :all_workers_available, state_data}
  end

  defp transition(:all_workers_available, state_data, true, true, true, true, true, _, _) do
    {:next_state, :all_workers_available, state_data}
  end

  # Key worker was checked in, pending work is available.
  defp transition(:await_writers, %{mode: :w} = state_data, _, _, _, _, false, q, k) when not is_nil k and not is_nil(q) do
    SpaghettiPool.handle_writes({:handle_pending, k}, state_data)
  end

  defp transition(:unlocked, %{mode: :w} = state_data, _, _, _, _, _, _, _) do
    SpaghettiPool.handle_writes(:handle_next, state_data)
  end

  defp transition(:handle_writes, %{mode: :w} = state_data, _, _, _, _, _, _, _) do
    SpaghettiPool.handle_writes(:handle_next, state_data)
  end

  defp transition(:unlocked, %{mode: :r} = state_data, _, _, _, _, _, _, _) do
    SpaghettiPool.handle_reads(:handle_next, state_data)
  end

  defp transition(:handle_reads, %{mode: :r} = state_data, _, _, _, _, _, _, _) do
    SpaghettiPool.handle_reads(:handle_next, state_data)
  end

  # Processing queue not empty
  defp transition(_, %{workers: [], mode: :r} = state_data, _, _, _, _, _, _, _) do
    {:next_state, :await_readers, state_data}
  end

  defp transition(_, %{mode: :r} = state_data, _, _, _, _, _, _, _) do
    SpaghettiPool.handle_reads(:handle_next, state_data)
  end

  defp transition(_, %{workers: [], mode: :w} = state_data, _, _, _, _, _, _, _) do
    {:next_state, :await_writers, state_data}
  end

  defp transition(_, %{mode: :w} = state_data, _, _, _, _, _, _, _) do
    SpaghettiPool.handle_writes(:handle_next, state_data)
  end
end