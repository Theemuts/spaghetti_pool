defmodule SpaghettiPool.RequestFSM do
  @moduledoc false
  use SpaghettiPool.FSM

  ### Public API

  def checkout(pool, type) do
    {:ok, fsm} = start(%{pool: pool}, [])
    :gen_fsm.sync_send_event(fsm, {:request_worker, type})
  end

  ### GenFSM

  def idle(e, from, sd) do
    sd = Map.put(sd, :caller, from)
    do_handle_event(:idle, e, sd)
  end

  def awaiting_worker(e, sd) do
    do_handle_event(:awaiting_worker, e, sd)
  end

  ### GenFSM callbacks

  def init(state_data) do
    {:ok, :idle, state_data}
  end

  ### Helpers

  defp do_handle_event(:idle, {:request_worker, _} = e, %{pool: pool} = sd) do
    awaiting_worker({:worker, {:ok, :worker}}, sd)
  end

  defp do_handle_event(:awaiting_worker, {:worker, reply}, %{caller: caller} = sd) do
    :gen_fsm.reply(caller, reply)
    {:stop, :normal, sd}
  end
end



