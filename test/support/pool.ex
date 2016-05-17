defmodule SpaghettiPool.Support.Pool do
  @moduledoc false

  @doc false
  def start_link(pool_opts, worker_args) do
    {:ok, _} = Application.ensure_all_started(:spaghetti_pool)
    pool_name = Keyword.fetch!(pool_opts, :name)
    sup_name = Keyword.get(pool_opts, :supervisor_name)

    children = [SpaghettiPool.child_spec(pool_name, pool_opts, worker_args)]
    sup_opts = case sup_name do
       nil -> [strategy: :one_for_one]
       sup_name -> [strategy: :one_for_one, name: sup_name]
    end

    Supervisor.start_link(children, sup_opts)
  end
end