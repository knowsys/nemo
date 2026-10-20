# nemo-io

The format and network adapters of the Nemo rule engine: import and export of delimiter-separated
values, RDF serializations, JSON, Parquet, and the solutions of SPARQL endpoints, compressed or
not, local or remote.

It loads into the tables of [nemo-physical](../nemo-physical/README.md) and holds no rule-language
or engine logic. Remote resources (`http`), SPARQL endpoints (`sparql`), and Parquet (`parquet`)
are behind features.
