# SpaghettiPool
A worker pool library that helps you benefit from ETS table concurrency.

ETS tables allow both read and write concurrency, but this comes with zero
safety guarantees when writing to a single key concurrently. `SpaghettiPool`
is built using `Erlang`'s `:gen_fsm`-behaviour, and distinguishes between read
and write requests. Only a single write worker per key can be checked out at
any time, this restriction does not apply to read workers.

Requests are handled in batches; while handling writes only write workers will
be checked out, and vice versa while handling reads.

It is also possible to lock and unlock the pool, in order block all access to
the table while performing maintenance tasks.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `spaghetti_pool` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:spaghetti_pool, "~> 0.0.1"}]
    end
    ```

  2. Ensure `spaghetti_pool` is started before your application:

    ```elixir
    def application do
      [applications: [:spaghetti_pool]]
    end
    ```

## Usage

Below is a minimal example of how you can start using `SpaghettiPool` in your
application:

```elixir
defmodule MyApp.Mixfile do
  use Mix.Project

  def project do
    [app: :my_app,
     version: "0.0.1",
     elixir: "~> 1.3-dev",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  def application do
    [applications: [:logger, :spaghetti_pool],
     mod: {MyApp, []}]
  end

  defp deps do
    [{:spaghetti_pool, git: "https://github.com/theemuts/spaghetti_pool.git", branch: "master"}]
  end
end
```

```elixir
defmodule MyApp do
  @moduledoc false
  use Application

  @doc false
  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    {:ok, _} = Application.ensure_all_started(:spaghetti_pool)
    pool_opts = [name: {:local, MyApp.Pool}, worker_module: MyApp.Pool.Worker]
    worker_args = :ok

    children = [SpaghettiPool.child_spec(MyApp.Pool, pool_opts, worker_args)]

    Supervisor.start_link(children, [strategy: :one_for_one, name: MyApp.PoolSupervisor])
  end

  @doc false
  def read_something() do
    worker = SpaghettiPool.checkout(MyApp.Pool, :read)
    result = MyApp.Pool.Worker.do_something(worker)
    SpaghettiPool.checkin(MyApp.Pool, worker, :read)

    result
  end

  @doc false
  def write_something(key) do
    worker = SpaghettiPool.checkout(MyApp.Pool, {:write, key})
    result = MyApp.Pool.Worker.do_something(worker)
    SpaghettiPool.checkin(MyApp.Pool, worker, {:write, key})

    result
  end

  @doc false
  def read_something_transaction() do
    SpaghettiPool.transaction(MyApp.Pool, :read, &MyApp.Pool.Worker.do_something/1)
  end
end
```

```elixir
defmodule MyApp.Pool.Worker do
  @moduledoc false

  use GenServer

  @doc false
  def start_link(worker_args) do
    GenServer.start_link(__MODULE__, worker_args)
  end

  @doc false
  def init(_), do: {:ok, nil}

  @doc false
  def do_something(worker) do
    GenServer.call(worker, :do_something)
  end

  @doc false
  def handle_call(:do_something, _from, state) do
    {:reply, :something, state}
  end
end
```

By default, this pool will have 10 workers, a maximum overflow of 10 workers,
and these workers are assigned in a last-in-first-out manner. See the
documentation for more information and options.