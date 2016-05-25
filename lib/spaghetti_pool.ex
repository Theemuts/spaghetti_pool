defmodule SpaghettiPool do
  use SpaghettiPool.FSM

  alias SpaghettiPool.ETS
  alias SpaghettiPool.Transition

  @moduledoc """
  A `:gen_fsm`-based reimplementation of `:poolboy` for concurrently read and
  written ETS-tables.

  If you wish to replace `poolboy` with `SpaghettiPool`, you must replace
  calls to `:poolboy.child_spec/2` and `:poolboy.child_spec/3` with
  `SpaghettiPool.child_spec/2` and `SpaghettiPool.child_spec/3` respectively.

  The second replacement which is necessary is that `SpaghettiPool`
  distinguishes between readers and writers. A single key cannot safely be
  written to by two workers, but distinct keys can. Read access is assumed
  to be safe in general, if this is not true in your use case, always use
  write workers.

  Due to this blocking behaviour, all workers are checked out asynchronously,
  and the type (and key in case of a requested reader) must be known at
  checkout time. As a result, `checkout/3`, `transaction/4` and `checkin/3`
  take different arguments. The return value of `status/1` is different as well.
  See the documentation of those functions for more information.

  Additionally, this pool offers the functionality to lock and unlock a pool
  of workers.
  """

  @type pool :: atom
  @type key :: any
  @type worker_type :: :read | {:write, key}
  @type sp_timeout :: non_neg_integer
  @type worker :: pid
  @type transaction_fun :: (worker -> any)
  @type state_name :: atom
  @type state :: map
  @type pool_opts :: Keyword.t
  @type worker_args :: any
  @type child_spec :: Supervisor.child_spec

  @typep start :: {:ok, :all_workers_available, state}
  @typep handle_next_read :: :handle_next
  @typep handle_next_write :: :handle_next| {:handle_pending, key}
  @typep in_queue :: {from, reference, reference, key}
  @typep silent_transition :: {:next_state, atom, map}
  @typep transition :: {:reply, any, state_name, state} | silent_transition
  @typep request :: {atom, reference | pid, :read | {:write, any} | map} | {:lock, reference} | atom
  @typep from :: {pid, reference}

  @timeout 5_000
  @new_state %{supervisor: nil, workers: [], current_write: MapSet.new,
               pending_write: %{}, processing_queue: :queue.new,
               read_queue: :queue.new, write_queue: :queue.new, monitors: nil,
               size: nil, overflow: 0, max_overflow: nil, strategy: nil,
               locked_by: nil, mode: :r}

  ### Public API

  @doc """
  Checkout a worker with given timeout.

  This function expects three arguments:
    - `pool`: the name of the pool the worker belongs to.
    - `type`: either `:read` or `{:write, key}`.
    - `timeout`: the maximum time spent waiting for a worker, defaults to
    5000 milliseconds.

  This function returns the `pid` of your worker, or times out.

  Reusing checked out read workers to read multiple keys is safe, but you
  should check out multiple write workers to handle multiple writes.
  Currently, it is not possible to checkout a single worker to handle multiple
  writes, or request multiple writers with a single call to checkout.
  """
  @spec checkout(pool, worker_type, sp_timeout) :: pid
  def checkout(pool, type, timeout \\ @timeout) do
    c_ref = make_ref()

    try do
      :gen_fsm.sync_send_all_state_event(pool, {:request_worker, c_ref, type}, timeout)
    rescue
      e ->
        :gen_fsm.send_all_state_event(pool, {:cancel_waiting, c_ref, type})
        raise e
    end
  end

  @doc """
  Check a worker back.

  This function expects three arguments:
    - `pool`: the name of the pool the worker belongs to.
    - `worker`: the worker `pid`.
    - `type`: either `:read` or `{:write, key}`.
  """
  @spec checkin(pool, worker, worker_type) :: :ok
  def checkin(pool, worker, type) do
    :gen_fsm.send_all_state_event(pool, {:checkin_worker, worker, type})
  end

  @doc """
  Checkout a worker, use it to perform a transaction, and check it back in.

  This function expects four arguments:
    - `pool`: the name of the pool the worker belongs to.
    - `type`: either `:read` or `{:write, key}`.
    - `fun`: an anonymous function with arity 1, the argument is the worker pid.
    - `timeout`: the maximum time spent waiting for a worker, defaults to
    5000 milliseconds.

  The result of the anonymous function is returned.
  """
  @spec transaction(pool, worker_type, transaction_fun, sp_timeout) :: any
  def transaction(pool, type, fun, timeout \\ @timeout) do
    worker = checkout(pool, type, timeout)

    try do
      fun.(worker)
    after
      :ok = checkin(pool, worker, type)
    end
  end

  @doc """
  Lock a pool. When a pool is locked, none of the workers can perform any
  action.

  This function expects two arguments:
    - `pool`: the name of the pool the worker belongs to.
    - `timeout`: the maximum time spent awaiting the lock, defaults to
    5000 milliseconds.
  """
  @spec lock(pool, sp_timeout) :: :ok
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

  @doc """
  Unlock a locked pool.

  This function expects one argument:
    - `pool`: the name of the pool the worker belongs to.
  """
  @spec unlock(pool) :: :ok
  def unlock(pool) do
    :gen_fsm.send_all_state_event(pool, :unlock_pool)
  end

  @doc """
  Get the pool's current state name and data.

  This function expects one argument:
    - `pool`: the name of the pool the worker belongs to.
  """
  @spec status(pool) :: {state_name, state}
  def status(pool), do: :gen_fsm.sync_send_all_state_event(pool, :status)

  @doc """
  Calls `:child_spec/3` with the same arguments for the pool and the worker.
  """
  @spec child_spec(pool, pool_opts) :: child_spec
  def child_spec(pool_name, args), do: child_spec(pool_name, args, args)

  @doc """
  A supervisor has children, this function generates the appropriate specifier
  for the pool workers. It expects three argument:
    - `pool_name`: the name of this pool.
    - `pool_args`: the arguments for this pool.
    - `workers_args`: the arguments for this pool's worker module.

  The second argument, `pool_args`, must be a keyword list which has the key
  `:worker_module`, having the worker module as its value.

  Three other options can be set in `pool_args`, besides the required
  `:worker_module`. These are:
    - `:size`: the minimum number of workers, 10 by default.
    -`:max_overflow`: the maximum number of additional workers, 10 by
    default.
    - `:strategy`: The worker assignment strategy, must be either `:fifo` or
    `:lifo`. Defaults to `:fifo`.

  If no third argument is given, the workers receive the same arguments as
  the pool.
  """
  @spec child_spec(atom, pool_opts, worker_args) :: child_spec
  def child_spec(pool_name, pool_args, worker_args) do
    {pool_name, {SpaghettiPool, :start_link, [pool_args, worker_args]}, :permanent, 5000, :worker, [SpaghettiPool]}
  end

  ### GenFSM

  ### GenFSM callbacks

  @doc false
  @spec start(pool_opts) :: start
  def start(pool_args), do: start(pool_args, pool_args)

  @doc false
  @spec start(pool_opts, worker_args) :: start
  def start(pool_args, worker_args), do: start_pool(:start, pool_args, worker_args)

  @doc false
  @spec start_link(pool_opts) :: start
  def start_link(pool_args), do: start_link(pool_args, pool_args)

  @doc false
  @spec start_link(pool_opts, worker_args) :: start
  def start_link(pool_args, worker_args), do: start_pool(:start_link, pool_args, worker_args)

  @doc false
  @spec stop(pool) :: :ok
  def stop(pool), do: :gen_fsm.sync_send_all_state_event(pool, :stop)

  ### Init

  @doc false
  @spec init({pool_opts, worker_args}) :: start
  def init({pool_args, worker_args}) do
    Process.flag(:trap_exit, true)
    mons = ETS.create_monitors_table

    mod =          Keyword.fetch!(pool_args, :worker_module)
    size =         Keyword.get(pool_args, :size, 10)
    max_overflow = Keyword.get(pool_args, :max_overflow, 10)
    strat =        Keyword.get(pool_args, :strategy, :lifo)

    unless strat in [:fifo, :lifo], do: raise "Invalid strategy. Choose :lifo or :fifo."

    {:ok, sup} = SpaghettiPoolSupervisor.start_link(mod, worker_args)
    state_data = %{@new_state | workers: prepopulate(size, sup), size: size,
                                strategy: strat, supervisor: sup,
                                max_overflow: max_overflow, monitors: mons}

    {:ok, :all_workers_available, state_data}
  end

  ### Handle reads

  @doc false
  # Handle next read if a worker is available, or the max overflow is not exceeded.
  # Start awaiting readers if queue is empty.
  @spec handle_reads(handle_next_read, state) :: transition
  def handle_reads(:handle_next, %{workers: w, processing_queue: queue, overflow: o, max_overflow: mo} = state_data)
      when length(w) > 0 or (mo > 0 and mo > o) do
    {state_name, state_data} = :queue.out(queue) |> handle_queue(state_data)
    Transition.transition(state_name, state_data)
  end

  # No workers available. Wait until one is.
  def handle_reads(:handle_next, %{workers: []} = state_data) do
    Transition.transition(:await_readers, state_data)
  end

  ### Handle writes

  @doc false
  # Handle next write if a worker is available, or the max overflow is not exceeded.
  # Start awaiting writers if queue is empty.
  # Add to pending write if other worker locked key.
  @spec handle_writes(handle_next_write, state) :: transition
  def handle_writes(:handle_next, %{workers: w, processing_queue: q, overflow: o, max_overflow: mo} = state_data)
      when length(w) > 0 or (mo > 0 and mo > o) do
    {state_name, state_data} = :queue.out(q) |> handle_queue(state_data)
    Transition.transition(state_name, state_data)
  end

  # No workers available. Wait until one is.
  def handle_writes(:handle_next, %{workers: []} = state_data) do
    Transition.transition(:await_writers, state_data)
  end

  # Assign the current worker to a pending write request.
  # Only called when a key has been unlocked.
  def handle_writes({:handle_pending, key}, state_data) do
    state_data = handle_pending(state_data, key)
    Transition.transition(:handle_writes, state_data)
  end

  # Pending writes are handled before a table is locked.
  def finish_writes({:handle_pending, key}, state_data) do
    state_data = handle_pending(state_data, key)
    Transition.transition(:pending_locked, state_data)
  end

  ### Lock acquired

  @doc false
  @spec locked(:all_workers_acquired, state) :: silent_transition
  def locked(:all_workers_acquired, %{locked_by: lb} = state_data) do
    :gen_fsm.reply(lb, :ok)
    Transition.transition(:locked, state_data)
  end

  ### Checkin workers

  @doc false
  @spec handle_event(request, state_name, state) :: silent_transition
  # Checkin a worker
  def handle_event({:checkin_worker, _, _} = e, state_name, state_data) do
    {key, state_data} = handle_checkin(e, state_data)
    Transition.transition(state_name, state_data, key)
  end

  ### Unlock pool

  def handle_event(:unlock_pool, _, state_data) do
    {:lock, state_data} = handle_unlock(state_data)
    Transition.transition(:unlocked, state_data)
  end

  ### Cancel waiting

  def handle_event({:cancel_waiting, c_ref, type}, state_name, state_data) do
    {key, state_data} = case ETS.match_and_demonitor(state_data, c_ref) do
      {:ok, state_data, pid} ->
        handle_checkin({:checkin_worker, pid, type}, state_data)
      {:error, state_data} ->
        key = tuple_get(type, 1, nil)
        {key, state_data}
    end

    Transition.transition(state_name, state_data, key)
  end

  ### Cancel lock

  def handle_event({:cancel_lock, l_ref}, _, state_data) do
    state_data = ETS.match_and_demonitor_lock(state_data, l_ref)
    Transition.transition(:unlocked, state_data)
  end

  ### Request worker

  @doc false
  @spec handle_sync_event(request, from, state_name, state) :: transition
  def handle_sync_event({:request_worker, _, :read} = e, from, :all_workers_available, state_data) do
    {pid, state_data} = handle_checkout(e, from, state_data)
    :gen_fsm.reply(from, pid)
    Transition.transition(:handle_reads, %{state_data | mode: :r})
  end

  def handle_sync_event({:request_worker, _, {:write, _}} = e, from, :all_workers_available, state_data) do
    {pid, state_data} = handle_checkout(e, from, state_data)
    :gen_fsm.reply(from, pid)
    Transition.transition(:handle_writes, %{state_data | mode: :w})
  end

  def handle_sync_event({:request_worker, _, :read} = e, from, state_name, state_data) do
    state_data = add_to_read_queue(e, from, state_data)
    Transition.transition(state_name, state_data)
  end

  def handle_sync_event({:request_worker, _, {:write, _}} = e, from, state_name, state_data) do
    state_data = add_to_write_queue(e, from, state_data)
    Transition.transition(state_name, state_data)
  end

  ### Lock pool

  def handle_sync_event({:lock_pool, l_ref}, {from_pid, _} = from, :all_workers_available, state_data) do
    m_ref = Process.monitor(from_pid)
    {_, _} = add_to_monitors_table(nil, l_ref, m_ref, state_data, :lock)
    Transition.transition(:inform_lock_success, %{state_data | locked_by: from})
  end

  def handle_sync_event({:lock_pool, _}, _, state_name, state_data) when state_name in [:pending_locked, :locked] do
    Transition.transition(:inform_lock_fail, state_data)
  end

  def handle_sync_event({:lock_pool, l_ref}, {from_pid, _} = from, _, state_data) do
    m_ref = Process.monitor(from_pid)
    {_, _} = add_to_monitors_table(nil, l_ref, m_ref, state_data, :lock)
    Transition.transition(:request_lock, %{state_data | locked_by: from})
  end

  ### Status

  def handle_sync_event(:status, from, state_name, state_data) do
    :gen_fsm.reply(from, {state_name, state_data})
    Transition.transition(state_name, state_data)
  end

  ### Stop

  def handle_sync_event(:stop, _, state_name, state_data) do
    {:stop, :normal, :ok, state_name, state_data}
  end

  def handle_sync_event(_, from, state_name, state_data) do
    :gen_fsm.reply(from, {:error, :invalid_message})
    Transition.transition(state_name, state_data)
  end

  ### Handle caller down

  @doc false
  @spec handle_info(tuple, state_name, state) :: silent_transition
  def handle_info({:"DOWN", m_ref, _, _, _}, state_name, state_data) do
    case handle_down(state_name, state_data, m_ref) do
      {:ok, state_data} -> Transition.transition(state_name, state_data)
      {:unlock, state_name, state_data} -> Transition.transition(state_name, state_data)
      {:ok, state_data, key} -> Transition.transition(state_name, state_data, key)
      :error -> raise "The locking process died. It is unsafe to continue, as the data might be inconsistent." # TODO: Let user handle error. Allow resolution function. Is it reall
    end
  end

  ### Handle worker exit

  def handle_info({:"EXIT", pid, _reason}, state_name, %{supervisor: sup} =state_data) do
    state_data = case ETS.lookup_and_demonitor_worker(state_data, pid) do
      {:ok, state_data, key} -> handle_worker_exit(pid, state_data, key)
      {:error, state_data} -> state_data
      {:error, %{workers: w} = state_data, true} ->
        w = Enum.filter(w, &(&1 != pid))
        %{state_data | workers: [new_worker(sup) | w]}
      val -> elem(val, 1)
    end

    Transition.transition(state_name, state_data)
  end

  def handle_info(_, state_name, state_data) do
    Transition.transition(state_name, state_data)
  end

  ### Terminate

  @doc false
  @spec terminate(any, state_name, state) :: :ok
  def terminate(_reason, _state_name, %{workers: workers, supervisor: sup}) do
    Enum.each(workers, fn(w) -> Process.unlink(w) end)
    Process.exit(sup, :shutdown)
    :ok
  end

  ## PRIVATE HELPERS

  @spec start_pool(:start | :start_link, pool_opts, worker_args) :: :gen_fsm.start | :gen_fsm.start_link
  defp start_pool(start_fun, pool_args, worker_args) do
    case Keyword.get(pool_args, :name) do
      nil ->
        apply(:gen_fsm, start_fun, [__MODULE__, {pool_args, worker_args}, []])
      name ->
        apply(:gen_fsm, start_fun, [name, __MODULE__, {pool_args, worker_args}, []])
    end
  end

  @spec prepopulate(integer, pid) :: list
  defp prepopulate(n, _) when n < 1, do: []
  defp prepopulate(n, sup), do: prepopulate(n, sup, [])

  @spec prepopulate(non_neg_integer, pid, list) :: list
  defp prepopulate(0, _sup, workers), do: workers
  defp prepopulate(n, sup, workers), do: prepopulate(n-1, sup, [new_worker(sup) | workers])

  @spec new_worker(pid) :: pid
  defp new_worker(sup) do
    {:ok, pid} = Supervisor.start_child(sup, [])
    true = Process.link(pid)
    pid
  end

  @spec new_worker(pid, pid) :: {pid, reference}
  defp new_worker(sup, from_pid) do
    pid = new_worker(sup)
    ref = Process.monitor(from_pid)
    {pid, ref}
  end

  @spec handle_checkout(request, from, state) :: {pid, state}
  defp handle_checkout({_, c_ref, type}, {from_pid, _}, %{workers: [pid|w]} = state_data) do
    m_ref = Process.monitor(from_pid)
    key = tuple_get(type, 1)

    pid
    |> add_to_monitors_table(c_ref, m_ref, state_data, key)
    |> update_current_write(key)
    |> update_workers(w)
  end

  defp handle_checkout({_, c_ref, type}, {from_pid, _}, %{supervisor: sup} = state_data) do
    {pid, m_ref} = new_worker(sup, from_pid)
    key = tuple_get(type, 1)

    pid
    |> add_to_monitors_table(c_ref, m_ref, state_data, key)
    |> update_current_write(key)
  end

  @spec handle_checkin(request, state) :: {key, state}
  defp handle_checkin({:checkin_worker, pid, type}, state_data) do
    key = tuple_get(type, 1)

    state_data
    |> maybe_remove_from_current_write(key)
    |> ETS.lookup_and_demonitor(pid, key)
    |> maybe_dismiss_worker
  end

  defp maybe_remove_from_current_write(state_data, nil), do: state_data
  defp maybe_remove_from_current_write(%{current_write: cw} = state_data, key) do
    %{state_data | current_write: MapSet.delete(cw, key)}
  end

  @spec handle_unlock(state) :: state
  defp handle_unlock(state_data) do
    ETS.lookup_and_demonitor(state_data, nil, :lock)
  end

  @spec handle_worker_exit(pid, state, key) :: state
  defp handle_worker_exit(pid, %{supervisor: sup, monitors: mons, overflow: o, processing_queue: q, workers: w} = state_data, _) do
    case :queue.out(q) do
      {{:value, {from, c_ref, m_ref, key}}, waiting} ->
        new_worker = new_worker(sup)
        true = ETS.insert(mons, {new_worker, c_ref, m_ref, key})
        :gen_fsm.reply(from, new_worker)
        %{state_data | processing_queue: waiting}
      {:empty, empty} when o > 0 ->
        %{state_data | overflow: o - 1, processing_queue: empty}
      {:empty, empty} ->
        w = [new_worker(sup) | Enum.filter(w, &(&1 != pid))]
        %{state_data | workers: w, processing_queue: empty}
    end
  end

  @spec add_to_read_queue(request, from, state) :: state
  defp add_to_read_queue({:request_worker, c_ref, :read}, {from_pid, _}  = from, %{read_queue: rq} = state_data) do
    m_ref = Process.monitor(from_pid)
    rq = :queue.in({from, c_ref, m_ref}, rq)
    %{state_data | read_queue: rq}
  end

  @spec add_to_write_queue(request, from, state) :: state
  defp add_to_write_queue({:request_worker, c_ref, {:write, key}}, {from_pid, _} = from, %{write_queue: wq} = state_data) do
    m_ref = Process.monitor(from_pid)
    wq = :queue.in({from, c_ref, m_ref, key}, wq)
    %{state_data | write_queue: wq}
  end

  @spec add_to_pending_write(in_queue, state) :: state
  defp add_to_pending_write({_, _, _, key} = val, %{pending_write: pw} = state_data) do
    pw = Map.update(pw, key, :queue.from_list([val]), fn(q) -> :queue.in(val, q) end)
    %{state_data | pending_write: pw}
  end

  @spec add_to_monitors_table(pid | nil, reference, reference, state, key) :: {pid, state}
  defp add_to_monitors_table(pid, c_ref, m_ref, %{monitors: mons, overflow: o, workers: w} = state_data, key) do
    true = ETS.insert(mons, {pid, c_ref, m_ref, key})
    case w do
      [] -> {pid, %{state_data | overflow: o + 1}}
      [_|w] -> {pid, %{state_data | workers: w}}
    end
  end

  @spec update_current_write({pid, state}, key) :: {pid, state}
  defp update_current_write({pid, state_data}, nil) do
    {pid, state_data}
  end

  defp update_current_write({pid, %{current_write: cw} = state_data}, key) do
    {pid, %{state_data | current_write: MapSet.put(cw, key)}}
  end

  @spec update_workers({pid, state}, list) :: {pid, state}
  defp update_workers({pid, state_data}, workers) do
    {pid, %{state_data | workers: workers}}
  end

  @spec maybe_dismiss_worker(state) :: state
  defp maybe_dismiss_worker(%{overflow: o, size: s, processing_queue: q, write_queue: wq, read_queue: rq} = state_data) when o > 0 do
    max_queue_size = q
    |> :queue.len
    |> max(:queue.len(rq))
    |> max(:queue.len(wq))

    if max_queue_size < o + s, do:  dismiss_worker(state_data), else: state_data
  end

  defp maybe_dismiss_worker(x), do: x

  @spec dismiss_worker(state) :: Supervisor.terminate_child
  defp dismiss_worker(%{supervisor: sup, workers: [pid|w]} = state_data) do
    true = Process.unlink(pid)
    Supervisor.terminate_child(sup, pid)
    %{state_data | workers: w}
  end

  defp handle_down(state_name, %{processing_queue: q, mode: m} = state_data, m_ref) do
    case ETS.match_down(state_data, m_ref) do
      [nil, :lock] when state_name == :locked ->
        :error
      [nil, :lock] when m == :w ->
        {:unlock, :await_writers, state_data}
      [nil, :lock] when m == :w ->
        {:unlock, :await_readers, state_data}
      [pid, key] ->
        type = if is_nil(key), do: :read, else: {:write, key}
        {_, state_data} = handle_checkin({:checkin_worker, pid, type}, state_data)
        {:ok, state_data}
      nil ->
        q = :queue.filter(fn({_, _, r, _}) -> r != m_ref end, q)
        {:ok, %{state_data | processing_queue: q}}
    end
  end

  defp tuple_get(tuple, index, default \\ nil)
  defp tuple_get(tuple, index, default) when tuple_size(tuple) <= index, do: default
  defp tuple_get(tuple, index, _) when is_tuple(tuple), do: elem(tuple, index)
  defp tuple_get(_, _, default), do: default


  defp handle_queue({:empty, _}, %{mode: :r} = state_data), do: {:await_readers, state_data}
  defp handle_queue({:empty, _}, %{mode: :w} = state_data), do: {:await_writers, state_data}

  defp handle_queue({{:value, {from, c_ref, _}}, queue}, %{mode: :r} = state_data) do
    {pid, state_data} = handle_checkout({:request_worker, c_ref, :read}, from, state_data)
    :gen_fsm.reply(from, pid)
    {:handle_reads, %{state_data | processing_queue: queue}}
  end

  defp handle_queue({{:value, {from, c_ref, _, key} = v}, queue}, %{current_write: cw, mode: :w} = state_data) do
    if MapSet.member?(cw, key) do
      {:handle_writes, add_to_pending_write(v, %{state_data | processing_queue: queue})}
    else
      {pid, state_data} = handle_checkout({:request_worker, c_ref, {:write, key}}, from, state_data)
      :gen_fsm.reply(from, pid)
      {:handle_writes, %{state_data | processing_queue: queue}}
    end
  end

  defp handle_pending(%{pending_write: pw} = state_data, key) do
    case :queue.out(pw[key]) do
      {:empty, _} ->
        %{state_data | pending_write: Map.delete(pw, key)}
      {{:value, {from, c_ref, _, key}}, {[], []}} ->
        {pid, state_data} = handle_checkout({:request_worker, c_ref, {:write, key}}, from, state_data)
        :gen_fsm.reply(from, pid)
        %{state_data | pending_write: Map.delete(pw, key)}
      {{:value, {from, c_ref, _, key}}, queue} ->
        {pid, state_data} = handle_checkout({:request_worker, c_ref, {:write, key}}, from, state_data)
        :gen_fsm.reply(from, pid)
        %{state_data | pending_write: %{pw | key => queue}}
    end
  end
end