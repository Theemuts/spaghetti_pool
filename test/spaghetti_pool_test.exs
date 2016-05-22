defmodule SpaghettiPoolTest do
  use ExUnit.Case, async: true
  doctest SpaghettiPool

  alias SpaghettiPool.Support.Pool
  alias SpaghettiPool.Support.Worker
  alias SpaghettiPool.Support.Util
  alias SpaghettiPool.Support.Request

  setup do
    name = Util.name
    opts = [worker_module: Worker, name: {:local, name}]
    {:ok, pid} = Pool.start_link(opts, [])

    {:ok, %{name: name, pid: pid}}
  end

  test "pool is initialized", %{name: name, pid: pid} do
    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    assert MapSet.equal?(state_data.current_write, MapSet.new)
    refute state_data.locked_by
    assert state_data.max_overflow == 10
    assert state_data.overflow == 0
    assert state_data.size == 10
    assert state_data.mode == :r
    assert state_data.monitors
    assert map_size(state_data.pending_write) == 0
    assert :queue.len(state_data.processing_queue) == 0
    assert :queue.len(state_data.read_queue) == 0
    assert :queue.len(state_data.write_queue) == 0
    assert state_data.strategy == :fifo
    assert length(state_data.workers) == 10
    assert is_pid(state_data.supervisor)
    Util.teardown(pid)
  end

  test "reader can be checked out", %{name: name, pid: pid} do
    assert is_pid(SpaghettiPool.checkout(name, :read))
    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_reads
    assert length(state_data.workers) == 9
    Util.teardown(pid)
  end

  test "reader can be checked in", %{name: name, pid: pid} do
    wid = SpaghettiPool.checkout(name, :read)
    assert SpaghettiPool.checkin(name, wid, :read) == :ok
    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    assert length(state_data.workers) == 10
    Util.teardown(pid)
  end

  test "writer can be checked out", %{name: name, pid: pid} do
    assert is_pid(SpaghettiPool.checkout(name, {:write, 1}))
    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_writes
    assert length(state_data.workers) == 9
    Util.teardown(pid)
  end

  test "writer can be checked in", %{name: name, pid: pid} do
    wid = SpaghettiPool.checkout(name, {:write, 1})
    assert SpaghettiPool.checkin(name, wid, {:write, 1}) == :ok
    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    assert length(state_data.workers) == 10
    Util.teardown(pid)
  end

  test "reader is checked in on client crash", %{name: name, pid: pid} do
    [r] = Util.workers(1)
    Request.request_worker(r, name, :read)
    Request.has_worker?(r)
    Process.exit(r, :kill)

    :timer.sleep(10) # Ensure DOWN message is received

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    assert length(state_data.workers) == 10
    assert is_pid(SpaghettiPool.checkout(name,  :read))

    Util.teardown(pid)
  end

  test "writer is checked in on client crash", %{name: name, pid: pid} do
    [r] = Util.workers(1)
    Request.request_worker(r, name, {:write, 1})
    Request.has_worker?(r)
    Process.exit(r, :kill)

    :timer.sleep(10) # Ensure DOWN message is received

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    assert length(state_data.workers) == 10
    assert is_pid(SpaghettiPool.checkout(name, {:write, 1}))
    Util.teardown(pid)
  end

  test "new reader is checked in on worker crash", %{name: name, pid: pid} do
    wid = SpaghettiPool.checkout(name, :read)
    Process.exit(wid, :kill)

    :timer.sleep(10) # Ensure DOWN message is received

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    assert length(state_data.workers) == 10
    Util.teardown(pid)
  end

  test "new writer is checked in on worker crash", %{name: name, pid: pid} do
    wid = SpaghettiPool.checkout(name, {:write, 1})
    Process.exit(wid, :kill)

    :timer.sleep(10) # Ensure DOWN message is received

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    assert length(state_data.workers) == 10
    Util.teardown(pid)
  end

  test "pool is locked immediately if no workers are checked out", %{name: name, pid: pid} do
    assert SpaghettiPool.lock(name) == :ok

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :locked

    assert SpaghettiPool.lock(name) == :error
    Util.teardown(pid)
  end

  test "pool is not locked until all read workers have returned", %{name: name, pid: pid} do
    [r1, r2, r3] = r = Util.workers(3)

    Request.request_worker(r1, name, :read)
    assert Request.has_worker?(r1)
    Request.request_worker(r2, name, :read)
    Request.request_worker(r3, name, :read)

    Request.return_worker(r1)
    Request.lock(r1, name)

    :timer.sleep(10)

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :pending_locked
    assert Request.has_worker?(r2)
    assert elem(state_data.locked_by, 0) == r1
    Request.return_worker(r2)

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :pending_locked

    Request.return_worker(r3)

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :locked

    Util.teardown(pid, r)
  end

  test "pool is not locked until all write workers have returned", %{name: name, pid: pid} do
    [r1, r2, r3] = r = Util.workers(3)

    Request.request_worker(r1, name, :read)
    Request.request_worker(r2, name, {:write, 1})
    Request.request_worker(r3, name, {:write, 2})

    assert Request.has_worker?(r1)
    Request.return_worker(r1)
    Request.lock(r1, name)

    :timer.sleep(10)

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :pending_locked
    assert Request.has_worker?(r2)
    assert elem(state_data.locked_by, 0) == r1
    Request.return_worker(r2)

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :pending_locked

    Request.return_worker(r3)

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :locked

    Util.teardown(pid, r)
  end

  test "pool is not locked until all write workers have returned, pending writes are handled first", %{name: name, pid: pid} do
    [r1, r2, r3] = r = Util.workers(3)

    Request.request_worker(r1, name, :read)
    Request.request_worker(r2, name, {:write, 1})
    Request.request_worker(r3, name, {:write, 1})

    assert Request.has_worker?(r1)
    Request.return_worker(r1)
    Request.lock(r1, name)

    :timer.sleep(10)

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :pending_locked
    assert Request.has_worker?(r2)
    assert elem(state_data.locked_by, 0) == r1
    Request.return_worker(r2)

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :pending_locked

    assert Request.has_worker?(r3)
    Request.return_worker(r3)

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :locked

    Util.teardown(pid, r)
  end

  test "pool can be unlocked", %{name: name, pid: pid} do
    assert SpaghettiPool.lock(name) == :ok
    assert SpaghettiPool.unlock(name) == :ok
    assert is_pid(SpaghettiPool.checkout(name, :read))
    Util.teardown(pid)
  end

  test "mode switches when all workers have been checked in", %{name: name, pid: pid} do
    [r1, r2] = r = Util.workers(2)

    Request.request_worker(r1, name, :read)
    assert Request.has_worker?(r1) # Always get this worker first

    Request.request_worker(r2, name, {:write, 1})

    :timer.sleep(10) # All requests must have been processed

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_reads
    assert :queue.len(state_data.write_queue) == 1

    Request.return_worker(r1) # Switch to write mode

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :handle_writes

    assert Request.has_worker?(r2)
    Request.return_worker(r2)

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available

    Util.teardown(pid, r)
  end

  test "concurrent read access is allowed", %{name: name, pid: pid} do
    [r1, r2, r3] = r = Util.workers(3)

    Request.request_worker(r1, name, :read)
    assert Request.has_worker?(r1) # Always get this worker first

    Request.request_worker(r2, name, :read)
    Request.request_worker(r3, name, :read)

    :timer.sleep(10) # All requests must have been processed

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_reads
    assert :queue.len(state_data.read_queue) == 2

    Request.return_worker(r1) # Start handling new reads, write queue is empty.

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_reads
    assert length(state_data.workers) == 8

    assert Request.has_worker?(r2)
    Request.return_worker(r2)
    assert Request.has_worker?(r3)
    Request.return_worker(r3)

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available

    Util.teardown(pid, r)
  end

  test "concurrent write access for different keys is allowed", %{name: name, pid: pid} do
    [r1, r2, r3] = r = Util.workers(3)

    Request.request_worker(r1, name, :read)
    assert Request.has_worker?(r1) # Always get this worker first

    Request.request_worker(r2, name, {:write, 1})
    Request.request_worker(r3, name, {:write, 2})

    :timer.sleep(10) # All requests must have been processed

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_reads
    assert :queue.len(state_data.write_queue) == 2

    Request.return_worker(r1) # Switch to write mode

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_writes
    assert map_size(state_data.pending_write) == 0
    assert MapSet.member?(state_data.current_write, 1)
    assert MapSet.member?(state_data.current_write, 2)

    assert Request.has_worker?(r2)
    Request.return_worker(r2)
    assert Request.has_worker?(r3)
    Request.return_worker(r3)

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    refute MapSet.member?(state_data.current_write, 1)
    assert map_size(state_data.pending_write) == 0

    Util.teardown(pid, r)
  end

  test "no concurrent access to the same key is allowed", %{name: name, pid: pid} do
    [r1, r2, r3] = r = Util.workers(3)

    Request.request_worker(r1, name, :read)
    assert Request.has_worker?(r1) # Always get this worker first

    Request.request_worker(r2, name, {:write, 1})
    Request.request_worker(r3, name, {:write, 1})

    :timer.sleep(10) # All requests must have been processed

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_reads
    assert :queue.len(state_data.write_queue) == 2

    Request.return_worker(r1) # Switch to write mode

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_writes
    assert map_size(state_data.pending_write) == 1
    assert MapSet.member?(state_data.current_write, 1)
    assert :queue.len(state_data.pending_write[1]) == 1

    assert Request.has_worker?(r2)
    Request.return_worker(r2)

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_writes
    assert MapSet.member?(state_data.current_write, 1)
    assert map_size(state_data.pending_write) == 1
    assert :queue.len(state_data.pending_write[1]) == 0

    assert Request.has_worker?(r3)
    Request.return_worker(r3)

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    refute MapSet.member?(state_data.current_write, 1)
    assert map_size(state_data.pending_write) == 0

    Util.teardown(pid, r)
  end

  test "switches to original mode if none of other in queue", %{name: name, pid: pid} do
    [r1, r2] = r = Util.workers(2)

    Request.request_worker(r1, name, :read)
    assert Request.has_worker?(r1) # Always get this worker first

    Request.request_worker(r2, name, :read)

    :timer.sleep(10) # All requests must have been processed

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :handle_reads
    assert :queue.len(state_data.read_queue) == 1

    Request.return_worker(r1) # Switch to write mode

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :handle_reads

    assert Request.has_worker?(r2)
    Request.return_worker(r2)

    {state_name, _} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available

    Util.teardown(pid, r)
  end

  test "status can be retrieved", %{name: name, pid: pid} do
    status = SpaghettiPool.status(name)
    assert tuple_size(status) == 2
    {name, data} = status
    assert is_atom(name)
    assert is_map(data)
    Util.teardown(pid)
  end

  test "new workers are created if overflow is not exceeded", %{name: name, pid: pid} do
    [r1 | workers] = r = Util.workers(12)
    Request.request_worker(r1, name, :read)
    assert Request.has_worker?(r1) # Always get this worker first
    Enum.map(workers, &Request.request_worker(&1, name, :read))
    Request.return_worker(r1)

    Enum.map(workers, assert &Request.has_worker?(&1))

    {_, state_data} = SpaghettiPool.status(name)
    assert state_data.overflow == 1

    Enum.map(workers, assert &Request.return_worker(&1))

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    assert state_data.overflow == 1

    Util.teardown(pid, r)
  end

  test "new readers are not created if overflow is exceeded", %{name: name, pid: pid} do
    [r1, w1, w2 | workers] = r = Util.workers(22)

    Request.request_worker(r1, name, :read)
    assert Request.has_worker?(r1) # Always get this worker first

    Enum.map([w2 | workers], &Request.request_worker(&1, name, :read))

    :timer.sleep(10) # Make sure next request arrives last.

    Request.request_worker(w1, name, :read)
    Request.return_worker(r1)

    Enum.map([w2 | workers], assert &Request.has_worker?(&1))
    {state_name, _} = SpaghettiPool.status(name)

    assert state_name == :await_readers
    Request.return_worker(w2) # one worker free, assigned immediately
    assert Request.has_worker?(w1)
    Enum.map(workers, assert &Request.return_worker(&1))
    Request.return_worker(w1)

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    assert state_data.overflow == 10

    Util.teardown(pid, r)
  end

  test "new writers are not created if overflow is exceeded", %{name: name, pid: pid} do
    [r1, w1, w2 | workers] = r = Util.workers(22)

    Request.request_worker(r1, name, :read)
    assert Request.has_worker?(r1) # Always get this worker first

    w = Enum.zip([w2 | workers], 1..length([w2 | workers]))
    Enum.map(w, &Request.request_worker(elem(&1, 0), name, {:write, elem(&1, 1)}))

    :timer.sleep(10) # Make sure next request arrives last.

    Request.request_worker(w1, name, {:write, length(w) + 1})
    Request.return_worker(r1)

    Enum.map([w2 | workers], assert &Request.has_worker?(&1))
    {state_name, _} = SpaghettiPool.status(name)

    assert state_name == :await_writers
    Request.return_worker(w2) # one worker free, assigned immediately
    assert Request.has_worker?(w1)
    Enum.map(workers, &Request.return_worker(&1))
    Request.return_worker(w1)

    {state_name, state_data} = SpaghettiPool.status(name)
    assert state_name == :all_workers_available
    assert state_data.overflow == 10

    Util.teardown(pid, r)
  end
end
