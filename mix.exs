defmodule Redix.Mixfile do
  use Mix.Project

  @description """
  Superfast, pipelined, resilient Redis driver for Elixir.
  """

  @repo_url "https://github.com/whatyouhide/redix"

  @version "0.2.0-dev"

  def project do
    [
      app: :redix,
      version: @version,
      elixir: "~> 1.0",
      build_embedded: Mix.env in [:prod, :bench],
      start_permanent: Mix.env == :prod,
      test_coverage: [tool: Coverex.Task],
      deps: deps,

      # Hex
      package: package,
      description: @description,

      # Docs
      name: "Redix",
      docs: [readme: "README.md", main: "README",
             source_ref: "v#{@version}",
             source_url: @repo_url],
    ]
  end

  def application do
    [applications: [:logger, :connection, :sbroker],
     mod: {Redix.App, []},
     env: [broker_client_queue: {:sbroker_timeout_queue, {:out, 5_000_000, :drop, 1024}},
           broker_worker_queue: {:sbroker_timeout_queue, {:out, 30_000_000, :drop, :infinity}},
           broker_timeout: 200]]
  end

  defp package do
    [
      contributors: ["Andrea Leopardi"],
      licenses: ["MIT"],
      links: %{"GitHub" => @repo_url},
    ]
  end

  defp deps do
    [
      {:connection, "~> 1.0.0-rc.1"},
      {:sbroker, "~> 0.7"},
      {:dialyze, "~> 0.2", only: :dev},
      {:benchfella, github: "alco/benchfella", only: :bench},
      {:redo, github: "heroku/redo", only: :bench},
      {:eredis, github: "wooga/eredis", only: :bench},
      {:yar, github: "dantswain/yar", only: :bench},
      {:coverex, "~> 1.4", only: :test},
      {:ex_doc, ">= 0.0.0", only: :docs},
    ]
  end
end
