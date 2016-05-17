defmodule SpaghettiPoolTest do
  use ExUnit.Case
  doctest SpaghettiPool

  alias SpaghettiPool.Support.Pool
  alias SpaghettiPool.Support.Worker

  test "worker module and pool name must be given to start" do
    opts = [worker_module: Worker, name: {:local, TestPool}]
    {ok, pid} = Pool.start_link(opts, [])
    assert ok == :ok
    assert Supervisor.stop(pid) == :ok
  end

  test "reader can be checked out" do
    opts = [worker_module: Worker, name: {:local, TestPool2}]
    {:ok, pid} = Pool.start_link(opts, [])
    assert is_pid(SpaghettiPool.checkout(TestPool2, :read))
    Supervisor.stop(pid)
  end

  test "reader can be checked in" do
    opts = [worker_module: Worker, name: {:local, TestPool3}]
    {:ok, pid} = Pool.start_link(opts, [])
    wid = SpaghettiPool.checkout(TestPool3, :read)
    assert SpaghettiPool.checkin(TestPool3, wid, :read) == :ok
    Supervisor.stop(pid)
  end

  test "writer can be checked out" do
    opts = [worker_module: Worker, name: {:local, TestPool4}]
    {:ok, pid} = Pool.start_link(opts, [])
    assert is_pid(SpaghettiPool.checkout(TestPool4, {:write, 1}))
    Supervisor.stop(pid)
  end

  test "writer can be checked in" do
    opts = [worker_module: Worker, name: {:local, TestPool5}]
    {:ok, pid} = Pool.start_link(opts, [])
    wid = SpaghettiPool.checkout(TestPool5, {:write, 1})
    assert SpaghettiPool.checkin(TestPool5, wid, {:write, 1}) == :ok
    Supervisor.stop(pid)
  end

  test "reader is checked in on client crash" do
    opts = [worker_module: Worker, name: {:local, TestPool6}]
    {:ok, pid} = Pool.start_link(opts, [])
    {:ok, tid} = Task.start(fn -> SpaghettiPool.checkout(TestPool6, :read) end)
    Process.exit(tid, :normal)
    assert is_pid(SpaghettiPool.checkout(TestPool6,  :read))
    Supervisor.stop(pid)
  end

  test "writer is checked in on client crash" do
    opts = [worker_module: Worker, name: {:local, TestPool7}]
    {:ok, pid} = Pool.start_link(opts, [])
    {:ok, tid} = Task.start(fn -> SpaghettiPool.checkout(TestPool7, {:write, 1}) end)
    Process.exit(tid, :normal)
    assert is_pid(SpaghettiPool.checkout(TestPool7,  {:write, 1}))
    Supervisor.stop(pid)
  end

  test "new reader is checked in on worker crash" do
    opts = [worker_module: Worker, name: {:local, TestPool8}]
    {:ok, pid} = Pool.start_link(opts, [])
    wid = SpaghettiPool.checkout(TestPool8, :read)
    Process.exit(wid, :kill)
    assert is_pid(SpaghettiPool.checkout(TestPool8,  :read))
    Supervisor.stop(pid)
  end

  test "new writer is checked in on worker crash" do
    opts = [worker_module: Worker, name: {:local, TestPool9}]
    {:ok, pid} = Pool.start_link(opts, [])
    wid = SpaghettiPool.checkout(TestPool9, {:write, 1})
    Process.exit(wid, :kill)
    assert is_pid(SpaghettiPool.checkout(TestPool9,  {:write, 1}))
    Supervisor.stop(pid)
  end
end
