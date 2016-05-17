defmodule SpaghettiPool do
  use SpaghettiPool.FSM

  @timeout 5_000

  ### Public API

  def checkout(pool, type), do: checkout(pool, type, @timeout)

  def checkout(pool, type, timeout) do
    c_ref = make_ref()

    try do
      :gen_fsm.sync_send_all_state_event(pool, {:request_worker, c_ref, type}, timeout)
    rescue
      e ->
        :gen_fsm.send_all_state_event(pool, {:cancel_waiting, c_ref, type})
        raise e
    end
  end

  def checkin(pool, worker, type), do: :gen_fsm.send_all_state_event(pool, {:checkin_worker, worker, type})

  def transaction(pool, type, fun), do: transaction(pool, type, fun, @timeout)

  def transaction(pool, type,fun, timeout) do
    worker = checkout(pool, type, timeout)

    try do
      fun.(worker)
    after
      :ok = checkin(pool, worker, type)
    end
  end

  def lock(pool, timeout \\ @timeout) do
    l_ref = make_ref()

    try do
      :gen_fsm.sync_send_all_state_event(pool, {:lock_pool, l_ref}, timeout)
    rescue
      e ->
        :gen_fsm.send_all_state_event(pool, {:cancel_lock, l_ref})
        raise e
    end
  end

  def unlock(pool) do
    :gen_fsm.send_all_state_event(pool, :unlock_pool)
  end

  def status(pool), do: :gen_fsm.sync_send_all_state_event(pool, :status)

  def child_spec(pool_id, pool_args), do: child_spec(pool_id, pool_args, [])

  def child_spec(pool_id, pool_args, worker_args) do
    {pool_id, {SpaghettiPool, :start_link, [pool_args, worker_args]}, :permanent, 5000, :worker, [SpaghettiPool]}
  end

  ### GenFSM

  ### GenFSM callbacks

  def start(pool_args), do: start(pool_args, pool_args)

  def start(pool_args, worker_args), do: start_pool(:start, pool_args, worker_args)

  def start_link(pool_args), do: start_link(pool_args, pool_args)

  def start_link(pool_args, worker_args), do: start_pool(:start_link, pool_args, worker_args)

  def stop(pool), do: :gen_fsm.sync_send_all_state_event(pool, :stop)

  ### Init

  def init({pool_args, worker_args}) do
    Process.flag(:trap_exit, true)
    mons = :ets.new(:monitors, [:private])

    state_data = %{supervisor: nil,
                   workers: [],
                   current_write: MapSet.new,
                   pending_write: %{},
                   processing_queue: :queue.new,
                   read_queue: :queue.new,
                   write_queue: :queue.new,
                   monitors: mons,
                   size: 10,
                   overflow: 0,
                   max_overflow: 10,
                   strategy: Keyword.get(pool_args, :max_overflow, :fifo),
                   locked_by: nil,
                   mode: :r}
    init(pool_args, worker_args, state_data)
  end

  def init([{:worker_module, mod} | rest], worker_args, state_data) do
    {:ok, sup} = SpaghettiPoolSupervisor.start_link(mod, worker_args)
    init(rest, worker_args, %{state_data | supervisor: sup})
  end

  def init([{:size, size} | rest], worker_args, state_data) do
    init(rest, worker_args, %{state_data | size: size})
  end

  def init([{:max_overflow, max_overflow} | rest], worker_args, state_data) do
    init(rest, worker_args, %{state_data | max_overflow: max_overflow})
  end

  def init([{:strategy, :lifo} | rest], worker_args, state_data) do
    init(rest, worker_args, %{state_data | strategy: :lifo})
  end

  def init([{:strategy, :fifo} | rest], worker_args, state_data) do
    init(rest, worker_args, %{state_data | strategy: :fifo})
  end

  def init([_ | rest], worker_args, state_data) do
    init(rest, worker_args, state_data)
  end

  def init([], _worker_args, %{size: size, supervisor: sup} = state_data) do
    workers = prepopulate(size, sup)
    {:ok, :all_workers_available, %{state_data | workers: workers}}
  end

  ### Handle reads

  def handle_reads(:handle_next, %{workers: w, processing_queue: queue, overflow: o, max_overflow: mo} = state_data)
      when length(w) > 0 or (mo > 0 and mo > o) do
    case :queue.out(queue) do
      {:empty, ^queue} ->
        transition(:handle_reads, state_data)
      {{:value, {from, c_ref, _}}, queue} ->
        {pid, state_data} = handle_checkout({:request_worker, c_ref, :read}, from, state_data)
        :gen_fsm.reply(from, pid)
        handle_reads(:handle_next, %{state_data | processing_queue: queue})
    end
  end

  def handle_reads(:handle_next, %{workers: []} = state_data) do
    {:next_state, :await_readers, state_data}
  end

  ### Handle writes

  def handle_writes(:handle_next, %{workers: w, processing_queue: queue, current_write: cw, overflow: o, max_overflow: mo} = state_data)
      when length(w) > 0 or (mo > 0 and mo > o) do
    case :queue.out(queue) do
      {:empty, ^queue} ->
        transition(:handle_writes, state_data)
      {{:value, {from, c_ref, _, key} = v}, queue} ->
        if MapSet.member?(cw, key) do
          add_to_pending_write(v, from, state_data)
        else
          {pid, state_data} = handle_checkout({:request_worker, c_ref, {:write, key}}, from, state_data)
          :gen_fsm.reply(from, pid)
          handle_writes(:handle_next, %{state_data | processing_queue: queue})
        end
    end
  end

  def handle_writes({:handle_pending, key}, %{workers: [pid|_], pending_write: pw} = state_data) do
    case :queue.out(pw[key]) do
      {:empty, _} ->
        handle_writes(:handle_next, %{state_data | pending_write: Map.delete(pw, key)})
      {{:value, {from, c_ref, _, key}}, queue} ->
        {pid, state_data} = handle_checkout({:request_worker, c_ref, {:write, key}}, from, state_data)
        :gen_fsm.reply(from, pid)
        handle_reads(:handle_next, %{state_data | pending_write: %{pw | key => queue}})
    end
  end

  def handle_writes(:handle_next, %{workers: []} = state_data) do
    {:next_state, :await_writers, state_data}
  end

  ### Lock acquired
  def locked(:all_workers_acquired, %{locked_by: lb} = state_data) do
    :gen_fsm.reply(lb, :ok)
    {:next_state, :locked, state_data}
  end

  ### Checkin workers

  def handle_event({:checkin_worker, _, _} = e, :handle_reads, state_data) do
    {_, state_data} = handle_checkin(e, state_data)
    handle_reads(:handle_next, state_data)
  end

  def handle_event({:checkin_worker, _, _} = e, :handle_writes, state_data) do
    {_, state_data} = handle_checkin(e, state_data)
    handle_writes(:handle_next, state_data)
  end

  def handle_event({:checkin_worker, _, _} = e, :await_readers, %{processing_queue: q, write_queue: wq} = state_data) do
    {nil, state_data} = handle_checkin(e, state_data)
    empty_processing = :queue.is_empty(q)
    all_available = all_workers_available?(state_data)
    empty_write = :queue.is_empty(wq)

    cond do
      empty_processing and all_available and empty_write -> {:next_state, :all_workers_available, state_data}
      empty_processing and all_available -> handle_writes(:handle_next, %{state_data | processing_queue: wq, write_queue: :queue.new, mode: :w})
      empty_processing -> {:next_state, :await_readers, state_data}
      true -> handle_reads(:handle_next, state_data)
    end
  end

  def handle_event({:checkin_worker, _, _} = e, :await_writers, %{processing_queue: q, read_queue: rq} = state_data) do
    {key, state_data} = handle_checkin(e, state_data)
    empty_processing = :queue.is_empty(q)
    all_available = all_workers_available?(state_data)
    empty_read = :queue.is_empty(rq)
    pending_writes = pending_writes?(state_data, key)

    cond do
      pending_writes -> handle_writes({:handle_pending, key}, state_data)
      empty_processing and all_available and empty_read -> {:next_state, :all_workers_available, state_data}
      empty_processing and all_available -> handle_reads(:handle_next, %{state_data | processing_queue: rq, read_queue: :queue.new, mode: :r})
      empty_processing -> {:next_state, :await_writers, state_data}
      true -> handle_writes(:handle_next, state_data)
    end
  end

  ### Unlock pool

  def handle_event(:unlock_pool, _, %{mode: mode} = state_data) do
    state_data = handle_unlock(state_data)

    case mode do
      :r -> handle_reads(:handle_next, state_data)
      :w -> handle_writes(:handle_next, state_data)
    end
  end

  ### Cancel waiting

  def handle_event({:cancel_waiting, c_ref, :read}, state_name, %{monitors: mons, processing_queue: q} = state_data) do
    case :ets.match(mons, {'$1', c_ref, '$2', nil}) do
      [[pid, m_ref]] ->
        Process.demonitor(m_ref, [:flush])
        true = :ets.delete(mons, pid)
        {nil, state_data} = handle_checkin({:checkin_worker, pid, :read}, %{monitors: mons, workers: w} = state_data)
        transition(state_name, state_data)
      [] ->
        cancel = fn
          ({_, ref, m_ref, _}) when ref == c_ref ->
            Process.demonitor(m_ref, [:flush])
            false
          (_) ->
            true
        end
        q = :queue.filter(cancel, q)
        transition(state_name, %{state_data | processing_queue: q})
    end
  end

  def handle_event({:cancel_waiting, c_ref, {:write, key}}, state_name, %{monitors: mons, processing_queue: q} = state_data) do
    case :ets.match(mons, {'$1', c_ref, '$2', key}) do
      [[pid, m_ref]] ->
        Process.demonitor(m_ref, [:flush])
        true = :ets.delete(mons, pid)
        {nil, state_data} = handle_checkin({:checkin_worker, pid, :read}, %{monitors: mons, workers: w} = state_data)
        pending_writes = pending_writes?(state_data, key)
        if pending_writes do
          handle_writes({:handle_pending, key}, state_data)
        else
          transition(state_name, state_data)
        end
      [] ->
        cancel = fn
          ({_, ref, m_ref, _}) when ref == c_ref ->
            Process.demonitor(m_ref, [:flush])
            false
          (_) ->
            true
        end
        q = :queue.filter(cancel, q)
        transition(state_name, %{state_data | processing_queue: q})
    end
  end

  ### Cancel lock

  def handle_event({:cancel_lock, l_ref}, _, %{mode: mode, monitors: mons} = state_data) do
    case :ets.match(mons, {nil, l_ref, '$2', nil}) do
      [[m_ref]] ->
        Process.demonitor(m_ref, [:flush])
        true = :ets.delete(mons, nil)
      [] ->
        true
    end

    case mode do
      :r -> transition(:handle_reads, state_data)
      :w -> transition(:handle_writes, state_data)
    end
  end

  ### Request worker

  def handle_sync_event({:request_worker, _, :read} = e, from, :all_workers_available, state_data) do
    {pid, state_data} = handle_checkout(e, from, state_data)
    :gen_fsm.reply(from, pid)
    handle_reads(:handle_next, state_data)
  end

  def handle_sync_event({:request_worker, _, {:write, _}} = e, from, :all_workers_available, state_data) do
    {pid, state_data} = handle_checkout(e, from, state_data)
    :gen_fsm.reply(from, pid)
    handle_writes(:handle_next, state_data)
  end

  def handle_sync_event({:request_worker, _, :read} = e, from, state_name, state_data) do
    state_data = add_to_read_queue(e, from, state_data)
    transition(state_name, state_data)
  end

  def handle_sync_event({:request_worker, _, {:write, _}} = e, from, state_name, state_data) do
    state_data = add_to_write_queue(e, from, state_data)
    transition(state_name, state_data)
  end

  ### Lock pool

  def handle_sync_event({:lock_pool, l_ref}, {from_pid, _} = from, :all_workers_available, state_data) do
    m_ref = Process.monitor(from_pid)
    add_to_monitors_table(nil, l_ref, m_ref, state_data, :lock)
    {:reply, :ok, :locked, %{state_data | locked_by: from}}
  end

  def handle_sync_event({:lock_pool, _}, _, state_name, state_data)
      when state_name in [:pending_locked, :locked] do
    {:reply, :error, state_name, state_data}
  end

  def handle_sync_event({:lock_pool, _}, from, _, state_data) do
    {:next_state, :pending_locked, %{state_data | locked_by: from}}
  end

  ### Status

  def handle_sync_event(:status, from, state_name, state_data) do
    :gen_fsm.reply(from, {state_name, state_data})
    transition(state_name, state_data)
  end

  ### Handle caller down

  def handle_info({:"DOWN", mon_ref, _, _, _}, state_name, %{monitors: mons, processing_queue: w, mode: mode} = state_data) do
    case :ets.match(mons, {'$1', '_', mon_ref, '$2'}) do
      [[pid, nil]] ->
        true = :ets.delete(mons, pid)
        state_data = handle_checkin({:checkin_worker, pid, :read}, state_data)
        transition(state_name, state_data)
      [[nil, :lock]] when state_name == :locked ->
        raise "The locking process died. It is unsafe to continue, as the data might be inconsistent."
      [[nil, :lock]] when state_name == :pending_locked and mode == :r->
        transition(state_name, state_data)
      [[pid, key]] ->
        true = :ets.delete(mons, pid)
        state_data = handle_checkin({:checkin_worker, pid, {:write, key}}, state_data)
        transition(state_name, state_data)
      [] ->
        w = :queue.filter(fn({_, _, r}) -> r != mon_ref end, w)
        transition(state_name, %{state_data | processing_queue: w})
    end
  end

  ### Handle worker exit

  def handle_info({:"EXIT", pid, _reason}, state_name, %{supervisor: sup, monitors: mons, workers: workers} = state_data) do
    case :ets.lookup(mons, pid) do
      [{^pid, _, mon_ref, key}] ->
        true = Process.demonitor(mon_ref)
        true = :ets.delete(mons, pid)
        state_data = handle_worker_exit(pid, state_data, key)
        transition(state_name, state_data)
      [] ->
        if Enum.member?(workers, pid) do
          workers = Enum.filter(workers, &(&1 != pid))
          transition(state_name, %{state_data | workers: [new_worker(sup) | workers]})
        else
          transition(state_name, state_data)
        end
    end
  end

  ### Terminate

  def terminate(_reason, _state_name, %{workers: workers, supervisor: sup}) do
    :ok = Enum.each(workers, fn(w) -> Process.unlink(w) end)
    true = Process.exit(sup, :shutdown)
    :ok
  end

  ## PRIVATE HELPERS

  defp start_pool(start_fun, pool_args, worker_args) do
    case Keyword.get(pool_args, :name) do
      nil ->
        apply(:gen_fsm, start_fun, [__MODULE__, {pool_args, worker_args}, []])
      name ->
        apply(:gen_fsm, start_fun, [name, __MODULE__, {pool_args, worker_args}, []])
    end
  end

  defp new_worker(sup) do
    {:ok, pid} = Supervisor.start_child(sup, [])
    true = Process.link(pid)
    pid
  end

  defp new_worker(sup, from_pid) do
    pid = new_worker(sup)
    ref = Process.monitor(from_pid)
    {pid, ref}
  end

  defp prepopulate(n, _) when n < 1 do
    []
  end

  defp prepopulate(n, sup) do
    prepopulate(n, sup, [])
  end

  defp prepopulate(0, _sup, workers) do
    workers
  end

  defp prepopulate(n, sup, workers) do
    prepopulate(n-1, sup, [new_worker(sup) | workers])
  end

  defp maybe_dismiss_worker(queue, %{overflow: o, workers: [_ | w] = workers} = state_data) when o > 0 do
    if :queue.len(queue) > length(workers) do
      dismiss_worker(state_data)
      %{state_data | workers: w}
    else
      state_data
    end
  end

  defp maybe_dismiss_worker(_, state_data), do: state_data

  defp dismiss_worker(%{supervisor: sup, workers: [pid|_]}) do
    true = Process.unlink(pid)
    Supervisor.terminate_child(sup, pid)
  end

  defp transition(state_name, %{processing_queue: q, write_queue: wq, read_queue: rq} = state_data) do
    all_available = all_workers_available?(state_data)
    idle = (:queue.len(rq) + :queue.len(wq)) == 0
    fully_processed = :queue.len(q) == 0

    case state_name do
      :pending_locked when all_available ->
        locked(:all_workers_acquired, state_data)
      :pending_locked ->
        {:next_state, :pending_locked, state_data}
      :locked ->
        {:next_state, :locked, state_data}
      _ when idle and all_available and fully_processed ->
        {:next_state, :all_workers_available, state_data}
      :await_readers when all_available and fully_processed ->
        handle_writes(:handle_next, %{state_data | processing_queue: wq, write_queue: :queue.new, mode: :w})
      :await_readers when all_available ->
        handle_reads(:handle_next, state_data)
      :await_writes when all_available and fully_processed ->
        handle_reads(:handle_next, %{state_data | processing_queue: rq, read_queue: :queue.new, mode: :r})
      :await_writers when all_available ->
        handle_writes(:handle_next, state_data)
      :handle_reads when all_available and fully_processed ->
        state_data = maybe_dismiss_worker(wq, state_data)
        handle_writes(:handle_next, %{state_data | processing_queue: wq, write_queue: :queue.new, mode: :w})
      :handle_writes when all_available and fully_processed ->
        state_data = maybe_dismiss_worker(rq, state_data)
        handle_reads(:handle_next, %{state_data | processing_queue: rq, read_queue: :queue.new, mode: :r})
      :handle_reads when all_available ->
        handle_reads(:handle_next, state_data)
      :handle_writes when all_available ->
        handle_writes(:handle_next, state_data)
      _ ->
        {:next_state, state_name, state_data}
    end
  end

  defp handle_checkin({:checkin_worker, pid, :read}, %{monitors: mons, workers: w} = state_data) do
    case :ets.lookup(mons, pid) do
      [{^pid, _, m_ref, nil}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, pid)
        {nil, %{state_data | workers: [pid|w]}}
      [] ->
        {nil, state_data}
    end
  end

  defp handle_checkin({:checkin_worker, pid, {:write, key}}, %{monitors: mons, workers: w} = state_data) do
    case :ets.lookup(mons, pid) do
      [{^pid, _, m_ref, ^key}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, pid)
        {key, %{state_data | workers: [pid|w]}}
      [] ->
        {key, state_data}
    end
  end

  defp handle_unlock(%{monitors: mons} = state_data) do
    case :ets.lookup(mons, nil) do
      [{nil, _, m_ref, :lock}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, nil)
        %{state_data | locked_by: nil}
      [] ->
        state_data
    end
  end

  defp add_to_read_queue(a, b, c) do
    {:request_worker, c_ref, :read} = a
    from = {from_pid, _} = b
    state_data = %{read_queue: rq} = c
    m_ref = Process.monitor(from_pid)
    rq = :queue.in({from, c_ref, m_ref}, rq)
    %{state_data | read_queue: rq}
  end

  defp add_to_write_queue({:request_worker, c_ref, {:write, key}}, {from_pid, _} = from, %{write_queue: wq} = state_data) do
    m_ref = Process.monitor(from_pid)
    wq = :queue.in({from, c_ref, m_ref, key}, wq)
    %{state_data | write_queue: wq}
  end

  defp add_to_pending_write({_, c_ref, {:write, key}, _}, {from_pid, _} = from, %{pending_write: pw} = state_data) do
    m_ref = Process.monitor(from_pid)
    val = {from, c_ref, m_ref, key}
    pw = Map.update(pw, key, :queue.from_list([val]), &:queue.in(val, &1))
    %{state_data | pending_write: pw}
  end

  defp handle_checkout({_, c_ref, :read}, {from_pid, _}, %{workers: [pid|w]} = state_data) do
    m_ref = Process.monitor(from_pid)
    pid
    |> add_to_monitors_table(c_ref, m_ref, state_data)
    |> update_workers(w)
  end

  defp handle_checkout({_, c_ref, {:write, key}}, {from_pid, _}, %{workers: [pid|w]} = state_data) do
    m_ref = Process.monitor(from_pid)
    pid
    |> add_to_monitors_table(c_ref, m_ref, state_data, key)
    |> update_current_write
    |> update_workers(w)
  end

  defp handle_checkout({_, c_ref, :read}, {from_pid, _}, %{workers: [], supervisor: sup} = state_data) do
    {pid, m_ref} = new_worker(sup, from_pid)
    add_to_monitors_table(pid, c_ref, m_ref, state_data)
  end

  defp handle_checkout({_, c_ref, {:write, key}}, {from_pid, _}, %{supervisor: sup} = state_data) do
    {pid, m_ref} = new_worker(sup, from_pid)
    pid
    |> add_to_monitors_table(c_ref, m_ref, state_data, key)
    |> update_current_write
  end

  def add_to_monitors_table(pid, c_ref, m_ref, %{monitors: mons, overflow: o, workers: w} = state_data, key \\ nil) do
    true = :ets.insert(mons, {pid, c_ref, m_ref, key})
    case w do
      [] -> {pid, %{state_data | overflow: o + 1}}
      [_|w] -> {pid, %{state_data | workers: w}}
    end
  end

  defp update_current_write({key, %{current_write: cw} = state_data}) do
    {key, %{state_data | current_write: MapSet.put(cw, key)}}
  end

  defp handle_worker_exit(pid, %{supervisor: sup, monitors: mons, overflow: o, processing_queue: q, workers: w} = state_data, nil) do
    case :queue.out(q) do
      {{:value, {from, c_ref, m_ref, key}}, waiting} ->
        new_worker = new_worker(sup)
        true = :ets.insert(mons, {new_worker, c_ref, m_ref, key})
        :gen_fsm.reply(from, new_worker)
        %{state_data | processing_queue: waiting}
      {:empty, empty} when o > 0 ->
        %{state_data | overflow: o - 1, processing_queue: empty}
      {:empty, empty} ->
        w = [new_worker(sup) | :lists.filter(fn (p) -> p != pid end, w)]
        %{state_data | workers: w, processing_queue: empty}
    end
  end

  defp handle_worker_exit(pid, %{pending_write: pw} = state_data, key) do
    case pw[key] do
      nil ->  handle_next_write(pid, state_data, key)
      pw_queue -> handle_pending_write(pw_queue, pid, state_data, key)
    end
  end

  defp handle_next_write(pid, %{current_write: cw, supervisor: sup, monitors: mons, overflow: o, processing_queue: q, workers: w} = state_data, key) do
    cw = MapSet.delete(cw, key)

    case :queue.out(q) do
      {{:value, {from, c_ref, m_ref, key}}, waiting} ->
        new_worker = new_worker(sup)
        true = :ets.insert(mons, {new_worker, c_ref, m_ref, key})
        :gen_fsm.reply(from, new_worker)
        %{state_data | processing_queue: waiting, current_write: MapSet.put(cw, key)}
      {:empty, empty} when o > 0 ->
        %{state_data | overflow: o - 1, processing_queue: empty}
      {:empty, empty} ->
        w = [new_worker(sup) | :lists.filter(fn (p) -> p != pid end, w)]
        %{state_data | workers: w, processing_queue: empty}
    end
  end

  defp handle_pending_write(pw_queue, pid, %{supervisor: sup, monitors: mons, overflow: o, pending_write: pw, workers: w} = state_data, key) do
    case :queue.out(pw_queue) do
      {{:value, {from, c_ref, m_ref, key}}, waiting} ->
        new_worker = new_worker(sup)
        true = :ets.insert(mons, {new_worker, c_ref, m_ref, key})
        :gen_fsm.reply(from, new_worker)
        %{state_data | pending_write: %{pw | key => waiting}}
      {:empty, _} when o > 0 ->
        handle_next_write(pid, %{state_data | overflow: o - 1, pending_write: Map.delete(pw, key)}, key)
      {:empty, _} ->
        w = [new_worker(sup) | :queue.filter(&(&1 != pid), w)]
        handle_next_write(pid, %{state_data | workers: w, pending_write: Map.delete(pw, key)}, key)
    end
  end

  defp all_workers_available?(%{supervisor: sup, workers: w}) do
    n_workers = sup |> Supervisor.which_children |> length
    n_avail = length(w)
    n_avail == n_workers
  end

  defp pending_writes?(%{pending_write: pw}, key) do
    case pw[key] do
      nil -> false
      _ -> true
    end
  end

  defp update_workers({key, state_data}, workers) do
    {key, %{state_data | workers: workers}}
  end
end