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
        :gen_fsm.send_all_state_event(pool, {:cancel_wating, c_ref})
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

  def status(pool), do: :gen_fsm.sync_send_all_state_event(pool, :status)

  ### Init

  def init({pool_args, worker_args}) do
    Process.flag(:trap_exit, true)
    mons = :ets.new(:monitors, [:private])

    state_data = %{supervisor: nil,
                   workers: [],
                   current_write: MapSet.new,
                   pending_write: %{},
                   processing_queue: nil,
                   read_queue: :queue.new,
                   write_queue: :queue.new,
                   monitors: mons,
                   size: 5,
                   overflow: 0,
                   max_overflow: 10,
                   strategy: :fifo}
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

  def handle_reads(:handle_next, %{workers: w, processing_queue: queue, overflow: o, max_overflow: mo} = sd)
      when length(w) > 0 or (mo > 0 and mo > o) do
    case :queue.out(queue) do
      {:empty, ^queue} -> {:next_state, :await_readers, sd}
      {{from, c_ref, _}, queue} ->
        {pid, sd} = handle_checkout({:request_worker, c_ref, :read, false}, from, sd)
        :gen_fsm.reply(from, pid)
        handle_reads(:handle_next, %{sd | processing_queue: queue})
    end
  end

  def handle_reads(:handle_next, %{workers: []} = sd) do
    {:next_state, :await_readers, sd}
  end

  ### Handle writes

  def handle_writes(:handle_next, %{workers: w, processing_queue: queue, current_write: cw, overflow: o, max_overflow: mo} = sd)
      when length(w) > 0 or (mo > 0 and mo > o) do
    # Only check queue. Pending writes get the old worker of that key on checkin.
    case :queue.out(queue) do
      {:empty, ^queue} ->
        {:next_state, :await_writers, sd}
      {{from, c_ref, _, key} = v, queue} ->
        if MapSet.member?(cw, key) do
          add_to_pending_write(v, from, sd)
        else
          {pid, sd} = handle_checkout({:request_worker, c_ref, {:write, key}}, from, sd)
          :gen_fsm.reply(from, pid)
          handle_reads(:handle_next, %{sd | processing_queue: queue})
        end
    end
  end

  def handle_writes(:handle_next, %{workers: []} = sd) do
    {:next_state, :await_writers, sd}
  end

  ### Checkin workers

  def handle_event({:checkin_worker, _, _} = e, :handling_reads, sd) do
    sd = handle_checkin(e, sd)
    {:next_state, :handling_reads, sd}
  end

  def handle_event({:checkin_worker, _, _} = e, :handling_writes, sd) do
    sd = handle_checkin(e, sd)
    {:next_state, :handling_writes, sd}
  end

  def handle_event({:checkin_worker, _, _} = e, :await_readers, %{supervisor: sup, processing_queue: q} = sd) do
    %{workers: w} = sd = handle_checkin(e, sd)

    if :queue.is_empty(q) do
      n_workers = sup |> Supervisor.which_children |> length
      n_avail = length(w)
      if n_avail == n_workers do
        handle_writes(:handle_next, %{sd | processing_queue: sd.write_queue, write_queue: :queue.new})
      else
        {:next_state, :await_readers, sd}
      end
    else
      handle_reads(:handle_next, sd)
    end
  end

  def handle_event({:checkin_worker, _, _} = e, :await_writers, %{supervisor: sup, processing_queue: q} = sd) do
    %{workers: w} = sd = handle_checkin(e, sd)

    if :queue.is_empty(q) do
      n_workers = sup |> Supervisor.which_children |> length
      n_avail = length(w)
      if n_avail == n_workers do
        handle_reads(:handle_next, %{sd | processing_queue: sd.read_queue, read_queue: :queue.new})
      else
        {:next_state, :await_writers, sd}
      end
    else
      handle_writes(:handle_next, sd)
    end
  end

  # Checkout Worker Request is a sync_send_all_state_event-op
  # Always put in queue, unless state is :all_workers_available
  def handle_sync_event({:request_worker, _, :read} = e, from, :all_workers_available, sd) do
    {pid, sd} = handle_checkout(e, from, sd)
    :gen_fm.reply(from, pid)
    handle_reads(:handle_next, sd)
  end

  def handle_sync_event({:request_worker, _, {:write, _}} = e, from, :all_workers_available, sd) do
    {pid, sd} = handle_checkout(e, from, sd)
    :gen_fm.reply(from, pid)
    handle_writes(:handle_next, sd)
  end

  def handle_sync_event({:request_worker, _, :read} = e, from, state_name, sd) do
    sd = add_to_read_queue(e, from, sd)
    {:next_state, state_name, sd}
  end

  def handle_sync_event({:request_worker, _, {:write, _}} = e, from, state_name, sd) do
    sd = add_to_write_queue(e, from, sd)
    {:next_state, state_name, sd}
  end

  # Something went down
  def handle_info({:"DOWN", mon_ref, _, _, _}, state_name, %{monitors: mons, waiting: w} = state_data) do
    case :ets.match(mons, {'$1', '_', mon_ref, '$2'}) do
      [[pid, nil]] ->
        true = :ets.delete(mons, pid)
        state_data = handle_checkin({:checkin, pid, :read}, state_data)
        {:next_state, state_name, state_data}
      [[pid, key]] ->
        true = :ets.delete(mons, pid)
        state_data = handle_checkin({:checkin, pid, {:write, key}}, state_data)
        {:next_state, state_name, state_data}
      [] ->
        w = :queue.filter(fn({_, _, r}) -> r != mon_ref end, w)
        {:next_state, state_name, %{state_data | waiting: w}}
    end
  end

  # Worker exit
  def handle_info({:"EXIT", pid, _reason}, state_name, %{supervisor: sup, monitors: mons, workers: workers} = state_data) do
    case :ets.lookup(mons, pid) do
      [{pid, _, mon_ref}] ->
        true = Process.demonitor(mon_ref)
        true = :ets.delete(mons, pid)
        state_data = handle_worker_exit(pid, state_data) #TODO
        {:next_state, state_name, state_data}
      [] ->
        if Enum.member?(workers, pid) do
          workers = Enum.filter(workers, &(&1 != pid))
          {:next_state, state_name, %{state_data | workers: [new_worker(sup) | workers]}}
        else
          {:next_state, state_name, state_data}
        end
    end
  end

  # Do nothing?
  def handle_info(_info, state_name, state_data) do
    {:next_state, state_name, state_data}
  end

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

  defp dismiss_worker(sup, pid) do
    true = Process.unlink(pid)
    Supervisor.terminate_child(sup, pid)
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

  defp handle_checkin({:checkin, pid, :read}, %{monitors: mons, workers: w} = sd) do
    case :ets.lookup(mons, pid) do
      [{^pid, _, m_ref, nil}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, pid)
        {nil, %{sd | workers: [pid|w]}}
      [] ->
        {nil, sd}
    end
  end

  defp handle_checkin({:checkin, pid, {:write, key}}, %{monitors: mons, workers: w} = sd) do
    case :ets.lookup(mons, pid) do
      [{^pid, _, m_ref, ^key}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, pid)
        {key, %{sd | workers: [pid|w]}}
      [] ->
        {key, sd}
    end
  end

  defp add_to_read_queue({:request_worker, c_ref, :read, _}, {from_pid, _} = from, %{read_queue: rq} = sd) do
    m_ref = Process.monitor(from_pid)
    rq = :queue.in({from, c_ref, m_ref}, rq)
    %{sd | read_queue: rq}
  end

  defp add_to_write_queue({:request_worker, c_ref, {:write, key}, _}, {from_pid, _} = from, %{write_queue: wq} = sd) do
    m_ref = Process.monitor(from_pid)
    wq = :queue.in({from, c_ref, m_ref, key}, wq)
    %{sd | write_queue: wq}
  end

  defp add_to_pending_write({_, c_ref, {:write, key}, _}, {from_pid, _} = from, %{pending_write: pw} = sd) do
    m_ref = Process.monitor(from_pid)
    val = {from, c_ref, m_ref, key}
    pw = Map.update(pw, key, :queue.from_list([val]), &:queue.in(val, &1))
    %{sd | pending_write: pw}
  end

  defp handle_checkout({_, c_ref, :read}, {from_pid, _}, %{workers: [pid|_]} = sd) do
    m_ref = Process.monitor(from_pid)
    add_to_monitors_table(pid, c_ref, m_ref, sd)
  end

  defp handle_checkout({_, c_ref, {:write, key}, _}, {from_pid, _}, %{workers: [pid|_]} = sd) do
    m_ref = Process.monitor(from_pid)
    add_to_monitors_table(pid, c_ref, m_ref, sd, key)
  end

  defp handle_checkout({_, c_ref, :read}, {from_pid, _}, %{workers: [], supervisor: sup} = sd) do
    {pid, m_ref} = new_worker(sup, from_pid)
    add_to_monitors_table(pid, c_ref, m_ref, sd)
  end

  defp handle_checkout({_, c_ref, {:write, key}}, {from_pid, _}, %{supervisor: sup} = sd) do
    {pid, m_ref} = new_worker(sup, from_pid)
    add_to_monitors_table(pid, c_ref, m_ref, sd, key)
  end

  def add_to_monitors_table(pid, c_ref, m_ref, %{monitors: mons, overflow: o, workers: w} = sd, key \\ nil) do
    true = :ets.insert(mons, {pid, c_ref, m_ref, key})
    case w do
      [] -> {pid, %{sd | overflow: o + 1}}
      [_|w] -> {pid, %{sd | workers: w}}
    end
  end

  defp handle_worker_exit(pid, %{supervisor: sup, monitors: mons, overflow: o, processing_queue: q, workers: w} = sd) do
    case :queue.out(q) do
      {{:value, {from, c_ref, m_ref, key}}, waiting} ->
        new_worker = new_worker(sup)
        true = :ets.insert(mons, {new_worker, c_ref, m_ref, key})
        :gen_fsm.reply(from, new_worker)
        %{sd | processing_queue: waiting}
      {:empty, empty} when o > 0 ->
        %{sd | overflow: o - 1, waiting: empty}
      {:empty, empty} ->
        w = [new_worker(sup) | :lists.filter(fn (p) -> p != pid end, w)]
        %{sd | workers: w, waiting: empty}
    end
  end
end