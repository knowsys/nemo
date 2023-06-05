# nemo-physical

This crate defines low-level data structures and operations of `nemo`. Since it is not be expected nor intended to be used directly, its interface is rather technical.

## High level overview

In essence this crate implements a **trie** along with iterators called **scans** for efficient and composable database operations. Because of the nature of the used datastructure, operations can only be performed on numbers (integer or floating point). More complex values like strings will be stored in a dictionary or otherwise represented in form of helper tables.
