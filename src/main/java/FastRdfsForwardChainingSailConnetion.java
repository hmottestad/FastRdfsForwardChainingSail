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
import org.openrdf.sail.UpdateContext;
import org.openrdf.sail.inferencer.InferencerConnection;
import org.openrdf.sail.inferencer.fc.AbstractForwardChainingInferencerConnection;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

public class FastRdfsForwardChainingSailConnetion extends AbstractForwardChainingInferencerConnection {


    private final FastRdfsForwardChainingSail fastRdfsForwardChainingSail;
    private final NotifyingSailConnection connection;

    public FastRdfsForwardChainingSailConnetion(FastRdfsForwardChainingSail fastRdfsForwardChainingSail, InferencerConnection e) {
        super(fastRdfsForwardChainingSail, e);
        this.fastRdfsForwardChainingSail = fastRdfsForwardChainingSail;
        this.connection = e;
    }


    boolean inferredCleared = false;

    @Override
    public void clearInferred(Resource... contexts) throws SailException {
        super.clearInferred(contexts);
        inferredCleared = true;
    }

    @Override
    protected void doInferencing() throws SailException {
        if (!inferredCleared) {
            return;
        }

        prepareIteration();

        CloseableIteration<? extends Statement, SailException> statements = connection.getStatements(null, null, null, false);
        while (statements.hasNext()) {
            Statement next = statements.next();
            addStatement(false, next.getSubject(), next.getPredicate(), next.getObject(), next.getContext());
        }


    }


    @Override
    public void removeStatement(UpdateContext modify, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {

        super.removeStatement(modify, subj, pred, obj, contexts);


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
            fastRdfsForwardChainingSail
                .resolveTypes((IRI) object)
                .stream()
                .peek(inferredType -> {
                    if (fastRdfsForwardChainingSail.sesameCompliant && inferredType.equals(RDFS.CLASS)) {
                        addInferredStatement(subject, RDFS.SUBCLASSOF, RDFS.RESOURCE, resources);
                    }
                    inferRdfTypeSubject[0] = true;
                })
                .forEach(inferredType -> addInferredStatement(subject, RDF.TYPE, inferredType, resources));
        }

        fastRdfsForwardChainingSail
            .resolveProperties(predicate)
            .forEach(inferredProperty -> addInferredStatement(subject, inferredProperty, object, resources));


        if (object instanceof Resource) {
            fastRdfsForwardChainingSail
                .resolveRangeTypes(predicate)
                .stream()
                .peek(inferredType -> {
                    if (fastRdfsForwardChainingSail.sesameCompliant && inferredType.equals(RDFS.CLASS)) {
                        addInferredStatement(((Resource) object), RDFS.SUBCLASSOF, RDFS.RESOURCE, resources);
                    }
                    inferRdfTypeObject[0] = true;

                })
                .forEach(inferredType -> addInferredStatement(((Resource) object), RDF.TYPE, inferredType, resources));
        }


        fastRdfsForwardChainingSail
            .resolveDomainTypes((IRI) predicate)
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



    public void removeStatements(Resource resource, IRI iri, Value value, Resource... resources) throws SailException {
        throw new UnsupportedOperationException();

    }


    public void addStatement(UpdateContext updateContext, Resource resource, IRI iri, Value value, Resource... resources) throws SailException {
        throw new UnsupportedOperationException();


    }


}
