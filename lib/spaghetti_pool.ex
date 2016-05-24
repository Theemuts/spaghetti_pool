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
    #IO.puts "init({pool_args, worker_args}) "
    Process.flag(:trap_exit, true)
    mons = ETS.create_monitors_table

    mod =          Keyword.fetch!(pool_args, :worker_module)
    size =         Keyword.get(pool_args, :size, 10)
    max_overflow = Keyword.get(pool_args, :max_overflow, 10)
    strat =        Keyword.get(pool_args, :strategy, :fifo)

    unless strat in [:fifo, :lifo], do: raise "Invalid strategy. Choose :lifo or :fifo."

    {:ok, sup} = SpaghettiPoolSupervisor.start_link(mod, worker_args)
    state_data = %{@new_state | workers: prepopulate(size, sup), size: size,
                                strategy: strat, supervisor: sup,
                                max_overflow: max_overflow, monitors: mons}

    {:ok, :all_workers_available, state_data}
  end

  ### Handle reads

  @doc false
  @spec handle_reads(handle_next_read, state) :: transition
  def handle_reads(:handle_next, %{workers: w, processing_queue: queue, overflow: o, max_overflow: mo} = state_data)
      when length(w) > 0 or (mo > 0 and mo > o) do
    case :queue.out(queue) do
      {:empty, ^queue} ->
        Transition.transition(:await_readers, state_data)
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

  @doc false
  @spec handle_writes(handle_next_write, state) :: transition
  def handle_writes(:handle_next, %{workers: w, processing_queue: queue, current_write: cw, overflow: o, max_overflow: mo} = state_data)
      when length(w) > 0 or (mo > 0 and mo > o) do
    case :queue.out(queue) do
      {:empty, ^queue} ->
        Transition.transition(:await_writers, state_data)
      {{:value, {from, c_ref, _, key} = v}, queue} ->
        if MapSet.member?(cw, key) do
          state_data = add_to_pending_write(v, %{state_data | processing_queue: queue})
          handle_writes(:handle_next, state_data)
        else
          {pid, state_data} = handle_checkout({:request_worker, c_ref, {:write, key}}, from, state_data)
          :gen_fsm.reply(from, pid)
          handle_writes(:handle_next, %{state_data | processing_queue: queue})
        end
    end
  end

  def handle_writes(:handle_next, %{workers: []} = state_data) do
    #IO.puts "handle_writes(:handle_next, %{workers: []} = state_data) "
    {:next_state, :await_writers, state_data}
  end

  def handle_writes({:handle_pending, key}, %{workers: [pid|_], pending_write: pw} = state_data) do
    #IO.puts "handle_writes({:handle_pending, key}, %{workers: [pid|_], pending_write: pw} = state_data) "
    case :queue.out(pw[key]) do
      {:empty, _} ->
        handle_writes(:handle_next, %{state_data | pending_write: Map.delete(pw, key)})
      {{:value, {from, c_ref, _, key}}, queue} ->
        {pid, state_data} = handle_checkout({:request_worker, c_ref, {:write, key}}, from, state_data)
        :gen_fsm.reply(from, pid)
        handle_writes(:handle_next, %{state_data | pending_write: %{pw | key => queue}})
    end
  end

  def finish_writes({:handle_pending, key}, %{workers: [pid|_], pending_write: pw} = state_data) do
    #IO.puts "finish_writes({:handle_pending, key}, %{workers: [pid|_], pending_write: pw} = state_data) "
    case :queue.out(pw[key]) do
      {:empty, _} ->
        {:next_state, :pending_locked, %{state_data | pending_write: Map.delete(pw, key)}}
      {{:value, {from, c_ref, _, key}}, queue} ->
        {pid, state_data} = handle_checkout({:request_worker, c_ref, {:write, key}}, from, state_data)
        :gen_fsm.reply(from, pid)
        {:next_state, :pending_locked, %{state_data | pending_write: %{pw | key => queue}}}
    end
  end

  ### Lock acquired
  @doc false
  @spec locked(:all_workers_acquired, state) :: {:next_state, :locked, state}
  def locked(:all_workers_acquired, %{locked_by: lb} = state_data) do
    #IO.puts "locked(:all_workers_acquired, %{locked_by: lb} = state_data) "
    :gen_fsm.reply(lb, :ok)
    {:next_state, :locked, state_data}
  end

  ### Checkin workers

  @doc false
  @spec handle_event(request, state_name, state) :: silent_transition
  def handle_event({:checkin_worker, _, _} = e, :handle_reads, state_data) do
    #IO.puts "handle_event({:checkin_worker, _, _} = e, :handle_reads, state_data) "
    #IO.puts "abcd"
    {_, state_data} = handle_checkin(e, state_data)
    Transition.transition(:handle_reads, state_data)
  end

  def handle_event({:checkin_worker, _, _} = e, :handle_writes, %{pending_write: pw} = state_data) do
    #IO.puts "handle_event({:checkin_worker, _, _} = e, :handle_writes, %{pending_write: pw} = state_data) "
    #IO.puts "efgh"
    {key, state_data} = handle_checkin(e, state_data)
    case pw[key] do
      nil -> handle_writes(:handle_next, state_data)
      _ -> handle_writes({:handle_pending, key}, state_data)
    end
  end

  def handle_event({:checkin_worker, _, {:write, key}} = e, state_name, %{processing_queue: q, read_queue: rq, pending_write: pw, write_queue: wq, mode: :w} = state_data) do
    #IO.puts "handle_event({:checkin_worker, _, {:write, key}} = e, state_name, %{processing_queue: q, read_queue: rq, pending_write: pw, write_queue: wq, mode: :w} = state_data) "
    #IO.puts "ijkl"
    {^key, state_data} = handle_checkin(e, state_data)

    #IO.inspect pw[key]

    Transition.transition(state_name, state_data, key)#, empty_processing, all_available, empty_read, idle, fully_processed, :w, pw[key], key)
  end

  def handle_event({:checkin_worker, _, _} = e, state_name, %{processing_queue: q, write_queue: wq, read_queue: rq, pending_write: pw, mode: :r} = state_data) do
    #IO.puts "handle_event({:checkin_worker, _, _} = e, state_name, %{processing_queue: q, write_queue: wq, read_queue: rq, pending_write: pw, mode: :r} = state_data) "
    #IO.puts "mnop"
    {nil, state_data} = handle_checkin(e, state_data)

    Transition.transition(state_name, state_data)#, empty_processing, all_available, empty_write, idle, fully_processed, :r)
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

  def handle_event({:cancel_waiting, c_ref, type}, state_name, state_data) do
    {key, state_data} = case ETS.match_and_demonitor(state_data, c_ref) do
      {:ok, state_data, pid} ->
        handle_checkin({:checkin_worker, pid, type}, state_data)
      {:error, state_data} when tuple_size(type) == 1 ->
        {nil, state_data}
      {:error, state_data} ->
        {elem(type, 1), state_data}
    end

    Transition.transition(state_name, state_data, key)
  end

  ### Cancel lock

  def handle_event({:cancel_lock, l_ref}, _, %{mode: mode, monitors: mons} = state_data) do
    state_data = ETS.match_and_demonitor_lock(state_data, l_ref)

    case mode do
      :r -> Transition.transition(:handle_reads, state_data)
      :w -> Transition.transition(:handle_writes, state_data)
    end
  end

  ### Request worker

  @doc false
  @spec handle_sync_event(request, from, state_name, state) :: transition
  def handle_sync_event({:request_worker, _, :read} = e, from, :all_workers_available, state_data) do
    #IO.puts "handle_sync_event({:request_worker, _, :read} = e, from, :all_workers_available, state_data) "
    {pid, state_data} = handle_checkout(e, from, state_data)
    :gen_fsm.reply(from, pid)
    handle_reads(:handle_next, %{state_data | mode: :r})
  end

  def handle_sync_event({:request_worker, _, {:write, _}} = e, from, :all_workers_available, state_data) do
    #IO.puts "handle_sync_event({:request_worker, _, {:write, _}} = e, from, :all_workers_available, state_data) "
    {pid, state_data} = handle_checkout(e, from, state_data)
    :gen_fsm.reply(from, pid)
    handle_writes(:handle_next, %{state_data | mode: :w})
  end

  def handle_sync_event({:request_worker, _, :read} = e, from, state_name, state_data) do
    #IO.puts "handle_sync_event({:request_worker, _, :read} = e, from, state_name, state_data) "
    state_data = add_to_read_queue(e, from, state_data)
    Transition.transition(state_name, state_data)
  end

  def handle_sync_event({:request_worker, _, {:write, _}} = e, from, state_name, state_data) do
    #IO.puts "handle_sync_event({:request_worker, _, {:write, _}} = e, from, state_name, state_data) "
    state_data = add_to_write_queue(e, from, state_data)
    Transition.transition(state_name, state_data)
  end

  ### Lock pool

  def handle_sync_event({:lock_pool, l_ref}, {from_pid, _} = from, :all_workers_available, state_data) do
    #IO.puts "handle_sync_event({:lock_pool, l_ref}, {from_pid, _} = from, :all_workers_available, state_data) "
    #IO.puts ":(1"
    m_ref = Process.monitor(from_pid)
    {_, _} = add_to_monitors_table(nil, l_ref, m_ref, state_data, :lock)
    {:reply, :ok, :locked, %{state_data | locked_by: from}}
  end

  def handle_sync_event({:lock_pool, _}, _, state_name, state_data)
      when state_name in [:pending_locked, :locked] do
    #IO.puts ":(2"
    {:reply, :error, state_name, state_data}
  end

  def handle_sync_event({:lock_pool, _}, from, _, state_data) do
    #IO.puts "handle_sync_event({:lock_pool, _}, from, _, state_data) "
    #IO.puts "pending locked"
    {:next_state, :pending_locked, %{state_data | locked_by: from}}
  end

  ### Status

  def handle_sync_event(:status, from, state_name, state_data) do
    #IO.puts "handle_sync_event(:status, from, state_name, state_data) "
    :gen_fsm.reply(from, {state_name, state_data})
    Transition.transition(state_name, state_data)
  end

  ### Stop

  def handle_sync_event(:stop, _, state_name, state_data) do
    #IO.puts "handle_sync_event(:stop, _, state_name, state_data) "
    {:stop, :normal, :ok, state_name, state_data}
  end

  def handle_sync_event(_, from, state_name, state_data) do
    #IO.puts "handle_sync_event(_, from, state_name, state_data) "
    :gen_fsm.reply(from, {:error, :invalid_message})
    Transition.transition(state_name, state_data)
  end

  ### Handle caller down

  @doc false
  @spec handle_info(tuple, state_name, state) :: silent_transition
  def handle_info({:"DOWN", m_ref, _, _, _}, state_name, state_data) do
    case maybe_checkin(state_name, state_data, m_ref) do
      {:ok, state_data} -> Transition.transition(state_name, state_data)
      {:ok, state_data, key} -> Transition.transition(state_name, state_data, key)
      :error -> raise "The locking process died. It is unsafe to continue, as the data might be inconsistent."
    end
  end

  ### Handle worker exit

  def handle_info({:"EXIT", pid, _reason}, state_name, %{supervisor: sup} =state_data) do
    state_data = case ETS.lookup_and_demonitor(state_data, pid) do
      {:ok, state_data, key} ->
        handle_worker_exit(pid, state_data, key)
      {:error, %{workers: w} = state_data, true} ->
        w = Enum.filter(w, &(&1 != pid))
        %{state_data | workers: [new_worker(sup) | w]}
      {:error, state_data} ->
        state_data
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
    #IO.puts "terminate(_reason, _state_name, %{workers: workers, supervisor: sup}) "
    :ok = Enum.each(workers, fn(w) -> Process.unlink(w) end)
    true = Process.exit(sup, :shutdown)
    :ok
  end

  ## PRIVATE HELPERS

  @spec start_pool(:start | :start_link, pool_opts, worker_args) :: :gen_fsm.start | :gen_fsm.start_link
  defp start_pool(start_fun, pool_args, worker_args) do
    #IO.puts "start_pool(start_fun, pool_args, worker_args) "
    case Keyword.get(pool_args, :name) do
      nil ->
        apply(:gen_fsm, start_fun, [__MODULE__, {pool_args, worker_args}, []])
      name ->
        apply(:gen_fsm, start_fun, [name, __MODULE__, {pool_args, worker_args}, []])
    end
  end

  @spec prepopulate(integer, pid) :: list
  defp prepopulate(n, _) when n < 1 do
    #IO.puts "prepopulate(n, _) when n < 1 "
    []
  end

  defp prepopulate(n, sup) do
    #IO.puts "prepopulate(n, sup) "
    prepopulate(n, sup, [])
  end

  @spec prepopulate(non_neg_integer, pid, list) :: list
  defp prepopulate(0, _sup, workers) do
    #IO.puts "prepopulate(0, _sup, workers) "
    workers
  end

  defp prepopulate(n, sup, workers) do
    #IO.puts "prepopulate(n, sup, workers) "
    prepopulate(n-1, sup, [new_worker(sup) | workers])
  end

  @spec new_worker(pid) :: pid
  defp new_worker(sup) do
    #IO.puts "new_worker(sup) "
    {:ok, pid} = Supervisor.start_child(sup, [])
    true = Process.link(pid)
    pid
  end

  @spec new_worker(pid, pid) :: {pid, reference}
  defp new_worker(sup, from_pid) do
    #IO.puts "new_worker(sup, from_pid) "
    pid = new_worker(sup)
    ref = Process.monitor(from_pid)
    {pid, ref}
  end

  @spec handle_checkout(request, from, state) :: {pid, state}
  defp handle_checkout({_, c_ref, :read}, {from_pid, _}, %{workers: [pid|w]} = state_data) do
    #IO.puts "handle_checkout({_, c_ref, :read}, {from_pid, _}, %{workers: [pid|w]} = state_data) "
    m_ref = Process.monitor(from_pid)
    pid
    |> add_to_monitors_table(c_ref, m_ref, state_data)
    |> update_workers(w)
  end

  defp handle_checkout({_, c_ref, {:write, key}}, {from_pid, _}, %{workers: [pid|w]} = state_data) do
    #IO.puts "handle_checkout({_, c_ref, {:write, key}}, {from_pid, _}, %{workers: [pid|w]} = state_data) "
    m_ref = Process.monitor(from_pid)
    pid
    |> add_to_monitors_table(c_ref, m_ref, state_data, key)
    |> update_current_write(key)
    |> update_workers(w)
  end

  defp handle_checkout({_, c_ref, :read}, {from_pid, _}, %{workers: [], supervisor: sup} = state_data) do
    #IO.puts "handle_checkout({_, c_ref, :read}, {from_pid, _}, %{workers: [], supervisor: sup} = state_data) "
    {pid, m_ref} = new_worker(sup, from_pid)
    add_to_monitors_table(pid, c_ref, m_ref, state_data)
  end

  defp handle_checkout({_, c_ref, {:write, key}}, {from_pid, _}, %{supervisor: sup} = state_data) do
    #IO.puts "handle_checkout({_, c_ref, {:write, key}}, {from_pid, _}, %{supervisor: sup} = state_data) "
    {pid, m_ref} = new_worker(sup, from_pid)
    pid
    |> add_to_monitors_table(c_ref, m_ref, state_data, key)
    |> update_current_write(key)
  end

  @spec handle_checkin(request, state) :: {key, state}
  defp handle_checkin({:checkin_worker, pid, :read}, %{monitors: mons, workers: w} = state_data) do
    #IO.puts "handle_checkin({:checkin_worker, pid, :read}, %{monitors: mons, workers: w} = state_data) "
    case :ets.lookup(mons, pid) do
      [{^pid, _, m_ref, nil}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, pid)
        {nil, %{state_data | workers: [pid|w]}}
      [] ->
        {nil, %{state_data | workers: [pid|w]}}
    end
  end

  defp handle_checkin({:checkin_worker, pid, {:write, key}}, %{monitors: mons, workers: w, current_write: cw} = state_data) do
    #IO.puts "handle_checkin({:checkin_worker, pid, {:write, key}}, %{monitors: mons, workers: w, current_write: cw} = state_data) "
    state_data = %{state_data | current_write: MapSet.delete(cw, key)}

    case :ets.lookup(mons, pid) do
      [{^pid, _, m_ref, ^key}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, pid)
        {key, %{state_data | workers: [pid|w]}}
      [] ->
        {key, state_data}
    end
  end

  @spec handle_next_write(pid, state, key) :: state
  defp handle_next_write(pid, %{current_write: cw, supervisor: sup, monitors: mons, overflow: o, processing_queue: q, workers: w} = state_data, key) do
    #IO.puts "handle_next_write(pid, %{current_write: cw, supervisor: sup, monitors: mons, overflow: o, processing_queue: q, workers: w} = state_data, key) "
    case :queue.out(q) do
      {{:value, {from, c_ref, m_ref, ^key}}, waiting} ->
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

  @spec handle_pending_write(:queue.queue, pid, state, key) :: state
  defp handle_pending_write(pw_queue, pid, %{supervisor: sup, monitors: mons, overflow: o, pending_write: pw, workers: w, current_write: cw} = state_data, key) do
    #IO.puts "handle_pending_write(pw_queue, pid, %{supervisor: sup, monitors: mons, overflow: o, pending_write: pw, workers: w, current_write: cw} = state_data, key) "
    case :queue.out(pw_queue) do
      {{:value, {from, c_ref, m_ref, key}}, waiting} ->
        new_worker = new_worker(sup)
        true = :ets.insert(mons, {new_worker, c_ref, m_ref, key})
        :gen_fsm.reply(from, new_worker)

        if :queue.len(waiting) == 0 do
          %{state_data | pending_write: Map.delete(pw, key)}
        else
          %{state_data | pending_write: %{pw | key => waiting}}
        end

      {:empty, _} when o > 0 ->
        handle_next_write(pid, %{state_data | overflow: o - 1, pending_write: Map.delete(pw, key), current_write: MapSet.delete(cw, key)}, key)
      {:empty, _} ->
        w = [new_worker(sup) | :queue.filter(&(&1 != pid), w)]
        handle_next_write(pid, %{state_data | workers: w, pending_write: Map.delete(pw, key), current_write: MapSet.delete(cw, key)}, key)
    end
  end

  @spec handle_unlock(state) :: state
  defp handle_unlock(%{monitors: mons} = state_data) do
    #IO.puts "handle_unlock(%{monitors: mons} = state_data) "
    case :ets.lookup(mons, nil) do
      [{nil, _, m_ref, :lock}] ->
        true = Process.demonitor(m_ref)
        true = :ets.delete(mons, nil)
        %{state_data | locked_by: nil}
      [] ->
        state_data
    end
  end

  @spec handle_worker_exit(pid, state, key) :: state
  defp handle_worker_exit(pid, %{supervisor: sup, monitors: mons, overflow: o, processing_queue: q, workers: w} = state_data, nil) do
    #IO.puts "handle_worker_exit(pid, %{supervisor: sup, monitors: mons, overflow: o, processing_queue: q, workers: w} = state_data, nil) "
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

  defp handle_worker_exit(pid, %{pending_write: pw, current_write: cw} = state_data, key) do
    #IO.puts "handle_worker_exit(pid, %{pending_write: pw, current_write: cw} = state_data, key) "
    case pw[key] do
      nil ->  handle_next_write(pid, %{state_data | current_write: MapSet.delete(cw, key)}, key)
      pw_queue -> handle_pending_write(pw_queue, pid, state_data, key)
    end
  end

  @spec add_to_read_queue(request, from, state) :: state
  defp add_to_read_queue({:request_worker, c_ref, :read}, {from_pid, _}  = from, %{read_queue: rq} = state_data) do
    #IO.puts "add_to_read_queue({:request_worker, c_ref, :read}, {from_pid, _}  = from, %{read_queue: rq} = state_data) "
    m_ref = Process.monitor(from_pid)
    rq = :queue.in({from, c_ref, m_ref}, rq)
    %{state_data | read_queue: rq}
  end

  @spec add_to_write_queue(request, from, state) :: state
  defp add_to_write_queue({:request_worker, c_ref, {:write, key}}, {from_pid, _} = from, %{write_queue: wq} = state_data) do
    #IO.puts "add_to_write_queue({:request_worker, c_ref, {:write, key}}, {from_pid, _} = from, %{write_queue: wq} = state_data) "
    m_ref = Process.monitor(from_pid)
    wq = :queue.in({from, c_ref, m_ref, key}, wq)
    %{state_data | write_queue: wq}
  end

  @spec add_to_pending_write(in_queue, state) :: state
  defp add_to_pending_write({_, _, _, key} = val, %{pending_write: pw} = state_data) do
    #IO.puts "add_to_pending_write({_, _, _, key} = val, %{pending_write: pw} = state_data) "
    pw = Map.update(pw, key, :queue.from_list([val]), fn(q) -> :queue.in(val, q) end)
    %{state_data | pending_write: pw}
  end

  @spec add_to_monitors_table(pid | nil, reference, reference, state, key) :: {pid, state}
  defp add_to_monitors_table(pid, c_ref, m_ref, %{monitors: mons, overflow: o, workers: w} = state_data, key \\ nil) do
    #IO.puts "add_to_monitors_table(pid, c_ref, m_ref, %{monitors: mons, overflow: o, workers: w} = state_data, key \\ nil) "
    true = :ets.insert(mons, {pid, c_ref, m_ref, key})
    case w do
      [] -> {pid, %{state_data | overflow: o + 1}}
      [_|w] -> {pid, %{state_data | workers: w}}
    end
  end

  @spec update_current_write({pid, state}, key) :: {pid, state}
  defp update_current_write({pid, %{current_write: cw} = state_data}, key) do
    #IO.puts "update_current_write({pid, %{current_write: cw} = state_data}, key) "
    {pid, %{state_data | current_write: MapSet.put(cw, key)}}
  end

  @spec update_workers({pid, state}, list) :: {pid, state}
  defp update_workers({pid, state_data}, workers) do
    #IO.puts "update_workers({pid, state_data}, workers) "
    {pid, %{state_data | workers: workers}}
  end

  @spec maybe_dismiss_worker(:queue.queue, state) :: state
  defp maybe_dismiss_worker(queue, %{overflow: o, workers: [_ | w] = workers} = state_data) when o > 0 do
    #IO.puts "maybe_dismiss_worker(queue, %{overflow: o, workers: [_ | w] = workers} = state_data) when o > 0 "
    if :queue.len(queue) > length(workers) do
      case dismiss_worker(state_data) do
        {:error, _} -> state_data
        _ -> %{state_data | workers: w}
      end
    else
      state_data
    end
  end

  defp maybe_dismiss_worker(_, state_data), do: state_data

  @spec dismiss_worker(state) :: Supervisor.terminate_child
  defp dismiss_worker(%{supervisor: sup, workers: [pid|_]}) do
    #IO.puts "dismiss_worker(%{supervisor: sup, workers: [pid|_]}) "
    true = Process.unlink(pid)
    Supervisor.terminate_child(sup, pid)
  end

  @spec pending_writes?(state, key) :: boolean
  defp pending_writes?(%{pending_write: pw}, key), do: not is_nil(pw[key])

  defp maybe_checkin(state_name, %{processing_queue: q, mode: mode} = state_data, m_ref) do
    case ETS.match_down(state_data, m_ref) do
      [pid, nil] ->
        {_, state_data} = handle_checkin({:checkin_worker, pid, :read}, state_data)
        {:ok, state_data}
      [pid, key] ->
        {_, state_data} = handle_checkin({:checkin_worker, pid, {:write, key}}, state_data)
        {:ok, state_data, key}
      [nil, :lock] when state_name == :locked ->
        :error
      [nil, :lock] ->
        {:ok, state_data}
      nil ->
        q = :queue.filter(fn({_, _, r, _}) -> r != m_ref end, q)
        {:ok, %{state_data | processing_queue: q}}
    end
  end
end