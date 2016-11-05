/*
    FastRdfsForwardChainingSail - A fast RDFS inferencing SAIL for RDF4J / Sesame

    Copyright (C) 2016  Håvard Mikkelsen Ottestad

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

 */


import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.*;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.InferencerConnection;
import org.openrdf.sail.inferencer.fc.AbstractForwardChainingInferencerConnection;

import java.util.*;

public class FastRdfsForwardChainingSailConnetion extends AbstractForwardChainingInferencerConnection {


    private final FastRdfsForwardChainingSail fastRdfsForwardChainingSail;
    private final NotifyingSailConnection connection;


    public FastRdfsForwardChainingSailConnetion(FastRdfsForwardChainingSail fastRdfsForwardChainingSail, InferencerConnection e) {
        super(fastRdfsForwardChainingSail, e);
        this.fastRdfsForwardChainingSail = fastRdfsForwardChainingSail;
        this.connection = e;
    }

    void statementCollector(Statement statement) {
        Value object = statement.getObject();
        IRI predicate = statement.getPredicate();

        if (predicate.equals(RDFS.SUBCLASSOF)) {
            fastRdfsForwardChainingSail.subClassOfStatemenets.add(statement);
        } else if (predicate.equals(RDF.TYPE) && object.equals(RDF.PROPERTY)) {
            fastRdfsForwardChainingSail.propertyStatements.add(statement);
        } else if (predicate.equals(RDFS.SUBPROPERTYOF)) {
            fastRdfsForwardChainingSail.subPropertyOfStatemenets.add(statement);
        } else if (predicate.equals(RDFS.RANGE)) {
            fastRdfsForwardChainingSail.rangeStatemenets.add(statement);
        } else if (predicate.equals(RDFS.DOMAIN)) {
            fastRdfsForwardChainingSail.domainStatemenets.add(statement);
        }

    }

    void temp() {
        calculateSubClassOf(fastRdfsForwardChainingSail.subClassOfStatemenets);
        findProperties(fastRdfsForwardChainingSail.propertyStatements);
        calculateSubPropertyOf(fastRdfsForwardChainingSail.subPropertyOfStatemenets);

        calculateRangeDomain(fastRdfsForwardChainingSail.rangeStatemenets, fastRdfsForwardChainingSail.calculatedRange);
        calculateRangeDomain(fastRdfsForwardChainingSail.domainStatemenets, fastRdfsForwardChainingSail.calculatedDomain);


        fastRdfsForwardChainingSail.calculatedTypes.forEach((subClass, superClasses) -> {
            addInferredStatement(subClass, RDFS.SUBCLASSOF, subClass);

            superClasses.forEach(superClass -> {
                addInferredStatement(subClass, RDFS.SUBCLASSOF, superClass);
                addInferredStatement(superClass, RDFS.SUBCLASSOF, superClass);

            });
        });

        fastRdfsForwardChainingSail.calculatedProperties.forEach((sub, sups) -> {
            addInferredStatement(sub, RDFS.SUBPROPERTYOF, sub);

            sups.forEach(sup -> {
                addInferredStatement(sub, RDFS.SUBPROPERTYOF, sup);
                addInferredStatement(sup, RDFS.SUBPROPERTYOF, sup);

            });
        });

    }

    private Set<IRI> resolveTypes(IRI value) {
        Set<IRI> iris = fastRdfsForwardChainingSail.calculatedTypes.get(value);

        return iris != null ? iris : Collections.emptySet();
    }

    private Set<IRI> resolveProperties(IRI predicate) {
        Set<IRI> iris = fastRdfsForwardChainingSail.calculatedProperties.get(predicate);

        return iris != null ? iris : Collections.emptySet();
    }


    private Set<IRI> resolveRangeTypes(IRI predicate) {
        Set<IRI> iris = fastRdfsForwardChainingSail.calculatedRange.get(predicate);

        return iris != null ? iris : Collections.emptySet();
    }

    private Set<IRI> resolveDomainTypes(IRI predicate) {
        Set<IRI> iris = fastRdfsForwardChainingSail.calculatedDomain.get(predicate);

        return iris != null ? iris : Collections.emptySet();
    }


    private void calculateSubClassOf(List<Statement> subClassOfStatements) {
        subClassOfStatements.forEach(s -> {
            IRI subClass = (IRI) s.getSubject();
            if (!fastRdfsForwardChainingSail.calculatedTypes.containsKey(subClass)) {
                fastRdfsForwardChainingSail.calculatedTypes.put(subClass, new HashSet<>());
            }

            fastRdfsForwardChainingSail.calculatedTypes.get(subClass).add((IRI) s.getObject());

        });

        long prevSize = 0;
        final long[] newSize = {-1};
        while (prevSize != newSize[0]) {

            prevSize = newSize[0];

            newSize[0] = 0;

            fastRdfsForwardChainingSail.calculatedTypes.forEach((key, value) -> {
                List<IRI> temp = new ArrayList<IRI>();
                value.forEach(superClass -> {
                    temp
                        .addAll(resolveTypes(superClass));
                });

                value.addAll(temp);
                newSize[0] += value.size();
            });


        }
    }

    private void findProperties(List<Statement> propertyStatements) {
        propertyStatements.stream()
            .map(Statement::getSubject)
            .map(property -> ((IRI) property))
            .filter(property -> !fastRdfsForwardChainingSail.calculatedProperties.containsKey(property))
            .forEach(property -> {
                fastRdfsForwardChainingSail.calculatedProperties.put(property, new HashSet<>());
            });
    }


    private void calculateSubPropertyOf(List<Statement> subPropertyOfStatemenets) {

        subPropertyOfStatemenets.forEach(s -> {
            IRI subClass = (IRI) s.getSubject();
            IRI superClass = (IRI) s.getObject();
            if (!fastRdfsForwardChainingSail.calculatedProperties.containsKey(subClass)) {
                fastRdfsForwardChainingSail.calculatedProperties.put(subClass, new HashSet<>());
            }

            if (!fastRdfsForwardChainingSail.calculatedProperties.containsKey(superClass)) {
                fastRdfsForwardChainingSail.calculatedProperties.put(superClass, new HashSet<>());
            }

            fastRdfsForwardChainingSail.calculatedProperties.get(subClass).add((IRI) s.getObject());

        });


        long prevSize = 0;
        final long[] newSize = {-1};
        while (prevSize != newSize[0]) {

            prevSize = newSize[0];

            newSize[0] = 0;

            fastRdfsForwardChainingSail.calculatedProperties.forEach((key, value) -> {
                List<IRI> temp = new ArrayList<IRI>();
                value.forEach(superProperty -> {
                    temp.addAll(resolveProperties(superProperty));
                });

                value.addAll(temp);
                newSize[0] += value.size();
            });


        }
    }

    private void calculateRangeDomain(List<Statement> rangeOrDomainStatements, Map<IRI, HashSet<IRI>> calculatedRangeOrDomain) {

        rangeOrDomainStatements.forEach(s -> {
            IRI predicate = (IRI) s.getSubject();
            if (!fastRdfsForwardChainingSail.calculatedProperties.containsKey(predicate)) {
                fastRdfsForwardChainingSail.calculatedProperties.put(predicate, new HashSet<>());
            }

            if (!calculatedRangeOrDomain.containsKey(predicate)) {
                calculatedRangeOrDomain.put(predicate, new HashSet<>());
            }

            calculatedRangeOrDomain.get(predicate).add((IRI) s.getObject());

            if (!fastRdfsForwardChainingSail.calculatedTypes.containsKey(s.getObject())) {
                fastRdfsForwardChainingSail.calculatedTypes.put((IRI) s.getObject(), new HashSet<>());
            }

        });


        fastRdfsForwardChainingSail.calculatedProperties
            .keySet()
            .stream()
            .filter(key -> !calculatedRangeOrDomain.containsKey(key))
            .forEach(key -> calculatedRangeOrDomain.put(key, new HashSet<>()));

        long prevSize = 0;
        final long[] newSize = {-1};
        while (prevSize != newSize[0]) {

            prevSize = newSize[0];

            newSize[0] = 0;

            calculatedRangeOrDomain.forEach((key, value) -> {
                List<IRI> resolvedBySubProperty = new ArrayList<>();
                resolveProperties(key).forEach(newPredicate -> {
                    HashSet<IRI> iris = calculatedRangeOrDomain.get(newPredicate);
                    if (iris != null) {
                        resolvedBySubProperty.addAll(iris);
                    }

                });

                List<IRI> resolvedBySubClass = new ArrayList<>();
                value.addAll(resolvedBySubProperty);


                value.stream().map(this::resolveTypes).forEach(resolvedBySubClass::addAll);

                value.addAll(resolvedBySubClass);

                newSize[0] += value.size();
            });


        }
    }


    boolean inferredCleared = true;

    @Override
    public void clearInferred(Resource... contexts) throws SailException {
        super.clearInferred(contexts);
        inferredCleared = true;
    }

    @Override
    protected void doInferencing() throws SailException {
        if(fastRdfsForwardChainingSail.schema == null){
            try (CloseableIteration<? extends Statement, SailException> statements = connection.getStatements(null, null, null, false)) {
                while (statements.hasNext()) {
                    Statement next = statements.next();
                    statementCollector(next);
                }
            }
            temp();

        }

        if (!inferredCleared) {
            return;
        }

        prepareIteration();

        try (CloseableIteration<? extends Statement, SailException> statements = connection.getStatements(null, null, null, false)) {
            while (statements.hasNext()) {
                Statement next = statements.next();
                addStatement(false, next.getSubject(), next.getPredicate(), next.getObject(), next.getContext());
            }
        }
        inferredCleared = false;

    }


    @Override
    protected Model createModel() {
        return new Model() {
            @Override
            public Model unmodifiable() {
                return null;
            }

            @Override
            public Set<Namespace> getNamespaces() {
                return null;
            }

            @Override
            public void setNamespace(Namespace namespace) {

            }

            @Override
            public Optional<Namespace> removeNamespace(String s) {
                return null;
            }

            @Override
            public boolean contains(Resource resource, IRI iri, Value value, Resource... resources) {
                return false;
            }

            @Override
            public boolean add(Resource resource, IRI iri, Value value, Resource... resources) {
                return false;
            }

            @Override
            public boolean clear(Resource... resources) {
                return false;
            }

            @Override
            public boolean remove(Resource resource, IRI iri, Value value, Resource... resources) {
                return false;
            }

            @Override
            public Model filter(Resource resource, IRI iri, Value value, Resource... resources) {
                return null;
            }

            @Override
            public Set<Resource> subjects() {
                return null;
            }

            @Override
            public Set<IRI> predicates() {
                return null;
            }

            @Override
            public Set<Value> objects() {
                return null;
            }

            @Override
            public ValueFactory getValueFactory() {
                return null;
            }

            @Override
            public Iterator<Statement> match(Resource resource, IRI iri, Value value, Resource... resources) {
                return null;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public boolean contains(Object o) {
                return false;
            }

            @Override
            public Iterator<Statement> iterator() {
                return null;
            }

            @Override
            public Object[] toArray() {
                return new Object[0];
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return null;
            }

            @Override
            public boolean add(Statement statement) {
                return false;
            }

            @Override
            public boolean remove(Object o) {
                return false;
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                return false;
            }

            @Override
            public boolean addAll(Collection<? extends Statement> c) {
                return false;
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                return false;
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                return false;
            }

            @Override
            public void clear() {

            }
        };
    }

    protected void addAxiomStatements() throws SailException {

    }

    protected int applyRules(Model model) throws SailException {


        return 0;
    }


    public void addStatement(Resource subject, IRI predicate, Value object, Resource... resources) throws SailException {
        addStatement(true, subject, predicate, object, resources);
    }


    public void addStatement(boolean actuallyAdd, Resource subject, IRI predicate, Value object, Resource... resources) throws SailException {

        final boolean[] inferRdfTypeSubject = {false};
        final boolean[] inferRdfTypeObject = {false};

        if (fastRdfsForwardChainingSail.sesameCompliant) {
            addInferredStatement(subject, RDF.TYPE, RDFS.RESOURCE, resources);

            if (object instanceof Resource) {
                addInferredStatement((Resource) object, RDF.TYPE, RDFS.RESOURCE, resources);

            }
        }

        if (predicate.getNamespace().equals(RDF.NAMESPACE) && predicate.getLocalName().charAt(0) == '_') {

            try {
                int i = Integer.parseInt(predicate.getLocalName().substring(1));
                if (i >= 1) {
                    addInferredStatement(subject, RDFS.MEMBER, object, resources);

                    addInferredStatement(predicate, RDF.TYPE, RDFS.RESOURCE, resources);
                    addInferredStatement(predicate, RDF.TYPE, RDFS.CONTAINERMEMBERSHIPPROPERTY, resources);
                    addInferredStatement(predicate, RDF.TYPE, RDF.PROPERTY, resources);
                    addInferredStatement(predicate, RDFS.SUBPROPERTYOF, predicate, resources);
                    addInferredStatement(predicate, RDFS.SUBPROPERTYOF, RDFS.MEMBER, resources);

                }
            } catch (NumberFormatException e) {
                // Ignore exception.
            }

        }

        if (actuallyAdd) {
            connection.addStatement(subject, predicate, object, resources);

        }

        if (predicate.equals(RDF.TYPE)) {
            resolveTypes((IRI) object)
                .stream()
                .peek(inferredType -> {
                    if (fastRdfsForwardChainingSail.sesameCompliant && inferredType.equals(RDFS.CLASS)) {
                        addInferredStatement(subject, RDFS.SUBCLASSOF, RDFS.RESOURCE, resources);
                    }
                    inferRdfTypeSubject[0] = true;
                })
                .forEach(inferredType -> addInferredStatement(subject, RDF.TYPE, inferredType, resources));
        }

        resolveProperties(predicate)
            .forEach(inferredProperty -> addInferredStatement(subject, inferredProperty, object, resources));


        if (object instanceof Resource) {
            resolveRangeTypes(predicate)
                .stream()
                .peek(inferredType -> {
                    if (fastRdfsForwardChainingSail.sesameCompliant && inferredType.equals(RDFS.CLASS)) {
                        addInferredStatement(((Resource) object), RDFS.SUBCLASSOF, RDFS.RESOURCE, resources);
                    }
                    inferRdfTypeObject[0] = true;

                })
                .forEach(inferredType -> addInferredStatement(((Resource) object), RDF.TYPE, inferredType, resources));
        }


        resolveDomainTypes((IRI) predicate)
            .stream()
            .peek(inferredType -> {
                if (fastRdfsForwardChainingSail.sesameCompliant && inferredType.equals(RDFS.CLASS)) {
                    addInferredStatement(subject, RDFS.SUBCLASSOF, RDFS.RESOURCE, resources);
                }
                inferRdfTypeSubject[0] = true;

            })
            .forEach(inferredType -> addInferredStatement((subject), RDF.TYPE, inferredType, resources));

        if (inferRdfTypeSubject[0]) {
            addInferredStatement(subject, RDF.TYPE, RDFS.RESOURCE, resources);

        }

        if (inferRdfTypeObject[0]) {
            addInferredStatement(((Resource) object), RDF.TYPE, RDFS.RESOURCE, resources);

        }

    }


}
