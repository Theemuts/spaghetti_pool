defmodule SpaghettiPool.Mixfile do
  use Mix.Project

  @version "0.1.0"

  def project do
    [app: :spaghetti_pool,
     version: @version,
     elixir: "~> 1.2",
     elixirc_paths: elixirc_paths(Mix.env),
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps,
     description: description,
     package: package,
     name: "Spaghetti Pool",
     docs: [source_ref: "v#{@version}"],
     source_url: "https://github.com/theemuts/spaghetti_pool"]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger],
     mod: {SpaghettiPool.App, []}]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_),     do: ["lib"]

  defp deps do
    [{:dialyze, "~> 0.2.0", only: :dev},
     # Docs dependencies
     {:earmark, "~> 0.2", only: :dev},
     {:ex_doc, "~> 0.11", only: :dev}]
  end

  defp description do
    """
    A worker pool for concurrently read and written ETS tables.
    """
  end

  defp package do
    [name: :spaghetti_pool,
     files: ["lib", "mix.exs", "README*", "LICENSE*"],
     maintainers: ["Thomas van Doornmalen"],
     licenses: ["MIT"],
     links: %{"GitHub" => "https://github.com/theemuts/spaghetti_pool"}]
  end
end
