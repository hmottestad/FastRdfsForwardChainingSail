# FastRdfsForwardChainingSail
A fast RDFS inferencing SAIL for RDF4J / Sesame that is 20-30x faster than the built in reasoner.

# How to use

```Java
Repository schema = .... // Your RDFS schema needs to be loaded into a repository
MemoryStore memoryStore = new MemoryStore();

FastRdfsForwardChainingSail forwardChainingSail = new FastRdfsForwardChainingSail(memoryStore, schema, true);

// Or set the Sesame compatability flag to false for a slightly faster inferencing ignoring rule rdfs4a, rdfs4b and rdfs8
//  FastRdfsForwardChainingSail forwardChainingSail = new FastRdfsForwardChainingSail(memoryStore, schema, false);

SailRepository sailRepository = new SailRepository(forwardChainingSail);
```

