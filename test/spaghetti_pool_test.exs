defmodule SpaghettiPoolTest do
  use ExUnit.Case
  doctest SpaghettiPool

  alias SpaghettiPool.Support.Pool
  alias SpaghettiPool.Support.Worker
  alias SpaghettiPool.Support.Util

  setup do
    name = Util.name
    opts = [worker_module: Worker, name: {:local, name}]
    {:ok, pid} = Pool.start_link(opts, [])

    {:ok, %{name: name, pid: pid}}
  end

  test "reader can be checked out", %{name: name, pid: pid} do
    assert is_pid(SpaghettiPool.checkout(name, :read))
    Supervisor.stop(pid)
  end

  test "reader can be checked in", %{name: name, pid: pid} do
    wid = SpaghettiPool.checkout(name, :read)
    assert SpaghettiPool.checkin(name, wid, :read) == :ok
    Supervisor.stop(pid)
  end

  test "writer can be checked out", %{name: name, pid: pid} do
    assert is_pid(SpaghettiPool.checkout(name, {:write, 1}))
    Supervisor.stop(pid)
  end

  test "writer can be checked in", %{name: name, pid: pid} do
    wid = SpaghettiPool.checkout(name, {:write, 1})
    assert SpaghettiPool.checkin(name, wid, {:write, 1}) == :ok
    Supervisor.stop(pid)
  end

  test "reader is checked in on client crash", %{name: name, pid: pid} do
    {:ok, tid} = Task.start(fn -> SpaghettiPool.checkout(name, :read) end)
    Process.exit(tid, :normal)
    assert is_pid(SpaghettiPool.checkout(name,  :read))
    Supervisor.stop(pid)
  end

  test "writer is checked in on client crash", %{name: name, pid: pid} do
    {:ok, tid} = Task.start(fn -> SpaghettiPool.checkout(name, {:write, 1}) end)
    Process.exit(tid, :normal)
    assert is_pid(SpaghettiPool.checkout(name,  {:write, 1}))
    Supervisor.stop(pid)
  end

  test "new reader is checked in on worker crash", %{name: name, pid: pid} do
    wid = SpaghettiPool.checkout(name, :read)
    Process.exit(wid, :kill)
    assert is_pid(SpaghettiPool.checkout(name,  :read))
    Supervisor.stop(pid)
  end

  test "new writer is checked in on worker crash", %{name: name, pid: pid} do
    wid = SpaghettiPool.checkout(name, {:write, 1})
    Process.exit(wid, :kill)
    assert is_pid(SpaghettiPool.checkout(name,  {:write, 1}))
    Supervisor.stop(pid)
  end

  test "pool can be locked", %{name: name, pid: pid} do
    assert SpaghettiPool.lock(name) == :ok
    assert SpaghettiPool.lock(name) == :error
    Supervisor.stop(pid)
  end

  test "pool can be unlocked", %{name: name, pid: pid} do
    assert SpaghettiPool.lock(name) == :ok
    assert SpaghettiPool.unlock(name) == :ok
    assert is_pid(SpaghettiPool.checkout(name, :read))
    Supervisor.stop(pid)
  end

  test "mode switches when all workers have been checked in", %{name: name, pid: pid} do
    wid = SpaghettiPool.checkout(name, :read)
    [t1, t2] = Enum.map(1..2, fn(i) -> Task.async(fn -> SpaghettiPool.checkout(name, {:write, i}) end) end)
    :timer.sleep(50)
    SpaghettiPool.checkin(name, wid, :read)
    assert is_pid(Task.await(t1))
    assert is_pid(Task.await(t2))
    Supervisor.stop(pid)
  end

  test "switches to original mode if none of other in queue", %{name: name, pid: pid} do
    wid = SpaghettiPool.checkout(name, :read)
    [t1, t2] = Enum.map(1..2, fn(_) -> Task.async(fn -> SpaghettiPool.checkout(name, :read) end) end)
    :timer.sleep(50)
    SpaghettiPool.checkin(name, wid, :read)
    assert is_pid(Task.await(t1))
    assert is_pid(Task.await(t2))
    Supervisor.stop(pid)
  end

  test "status can be retrieved", %{name: name, pid: pid} do
    status = SpaghettiPool.status(name)
    assert tuple_size(status) == 2
    {name, data} = status
    assert is_atom(name)
    assert is_map(data)
    Supervisor.stop(pid)
  end
end
