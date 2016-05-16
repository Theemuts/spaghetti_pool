defmodule SpaghettiPool.App do
  @moduledoc false
  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
    ]

    opts = [strategy: :one_for_one, name: SpaghettiPool]
    Supervisor.start_link(children, opts)
  end

#-module(poolboy_sup).
#-behaviour(supervisor).
#
#-export([start_link/2, init/1]).
#
#start_link(Mod, Args) ->
#    supervisor:start_link(?MODULE, {Mod, Args}).
#
#init({Mod, Args}) ->
#    {ok, {{simple_one_for_one, 0, 1},
#          [{Mod, {Mod, start_link, [Args]},
#            temporary, 5000, worker, [Mod]}]}}.
end