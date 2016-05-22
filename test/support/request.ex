defmodule SpaghettiPool.Support.Request do
  @moduledoc false
  use GenServer

  def start, do: GenServer.start(__MODULE__, [])

  def init(_), do: {:ok, nil}

  def request_worker(pid, pool, type) do
    GenServer.cast(pid, {:request, pool, type})
  end

  def has_worker?(pid) do
    GenServer.call(pid, :has_worker)
  end

  def return_worker(pid) do
    GenServer.call(pid, :return_worker)
  end

  def lock(pid, pool) do
    GenServer.cast(pid, {:lock, pool})
  end

  def handle_cast({:request, pool, type}, _state) do
    wid = SpaghettiPool.checkout(pool, type)
    {:noreply, {wid, pool, type}}
  end

  def handle_cast({:lock, pool}, state) do
    SpaghettiPool.lock(pool)
    {:noreply, state}
  end

  def handle_call(:has_worker, _, wid), do: {:reply, not is_nil(wid), wid}

  def handle_call(:return_worker, _, nil) do
    {:reply, :ok, nil}
  end

  def handle_call(:return_worker, _, {wid, pool, type}) do
    SpaghettiPool.checkin(pool, wid, type)
    {:reply, :ok, nil}
  end
end