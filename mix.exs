defmodule Redix.Mixfile do
  use Mix.Project

  @description """
  Superfast, pipelined, resilient Redis driver for Elixir.
  """

  @repo_url "https://github.com/whatyouhide/redix"

  @version "0.3.6"

  def project() do
    [app: :redix,
     version: @version,
     elixir: "~> 1.0",
     build_embedded: Mix.env in [:prod, :bench],
     start_permanent: Mix.env == :prod,
     deps: deps(),

     # Hex
     package: package(),
     description: @description,

     # Docs
     name: "Redix",
     docs: [main: "Redix",
            source_ref: "v#{@version}",
            source_url: @repo_url,
            extras: ["README.md", "pages/Reconnections.md", "pages/Real world usage.md"]]]
  end

  def application() do
    [applications: [:logger, :connection, :sbroker]]
  end

  defp package() do
    [maintainers: ["Andrea Leopardi", "Aleksei Magusev"],
     licenses: ["MIT"],
     links: %{"GitHub" => @repo_url}]
  end

  defp deps() do
    [{:connection, "~> 1.0.0"},
     {:sbroker, "~> 1.0.0-beta.2"},
     {:backoff, "~> 1.1"},
     {:dialyze, "~> 0.2", only: :dev},
     {:markdown, github: "devinus/markdown", only: :docs},
     {:ex_doc, ">= 0.0.0", only: :docs}]
  end
end
