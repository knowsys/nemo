# nemo-physical

The database layer of the Nemo rule engine: the in-memory tables and the operations on them.

Values are stored inline where they fit and in a dictionary otherwise. Tables are immutable
columnar tries, kept by a table manager in the column orders the computations need. Joins,
filters, and the other operations are described as operation plans and executed against the
table manager, in parallel where the work is large enough.

The crate serves the logical layer, [nemo](../nemo/README.md), and its interface is shaped
by that use. To use Nemo as a library, depend on `nemo`.
