defmodule SpaghettiPoolTest do
  use ExUnit.Case, async: false
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
    {ok, pid} = Pool.start_link(opts, [])
    assert is_pid(SpaghettiPool.checkout(TestPool2, :read))
    assert Supervisor.stop(pid) == :ok
  end

  test "reader can be checked in" do
    opts = [worker_module: Worker, name: {:local, TestPool3}]
    {ok, pid} = Pool.start_link(opts, [])
    assert is_pid(SpaghettiPool.checkout(TestPool3, :read))
    assert SpaghettiPool.checkin(TestPool3, pid, :read) == :ok
    assert Supervisor.stop(pid) == :ok
  end

  test "writer can be checked out" do
    opts = [worker_module: Worker, name: {:local, TestPool4}]
    {ok, pid} = Pool.start_link(opts, [])
    assert is_pid(SpaghettiPool.checkout(TestPool4, {:write, 1}))
    assert Supervisor.stop(pid) == :ok
  end

  test "reader can be checked in" do
    opts = [worker_module: Worker, name: {:local, TestPool5}]
    {ok, pid} = Pool.start_link(opts, [])
    assert is_pid(SpaghettiPool.checkout(TestPool5, {:write, 1}))
    assert SpaghettiPool.checkin(TestPool5, pid, {:write, 1}) == :ok
    assert Supervisor.stop(pid) == :ok
  end
end
