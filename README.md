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
sailRepository.initialize();

```

# How it works
The basis of FastRdfsForwardChainingSail is a set of precomputed hashmaps for quickly looking up types and properties so that 
the incoming data can be streamed through the reasoner. The predicate of each triple is looked up in a domain and a range hashmap to 
apply the correct domain and range types, and triples with rdf:type are looked up in a sub-class hashmap for retriving the new types.
