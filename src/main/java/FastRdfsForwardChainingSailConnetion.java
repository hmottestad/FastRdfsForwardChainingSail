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
import org.openrdf.IsolationLevel;
import org.openrdf.model.*;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.UnknownSailTransactionStateException;
import org.openrdf.sail.UpdateContext;
import org.openrdf.sail.inferencer.InferencerConnection;
import org.openrdf.sail.inferencer.fc.AbstractForwardChainingInferencerConnection;

public class FastRdfsForwardChainingSailConnetion extends AbstractForwardChainingInferencerConnection {


    private final FastRdfsForwardChainingSail fastRdfsForwardChainingSail;
    private final NotifyingSailConnection connection;

    public FastRdfsForwardChainingSailConnetion(FastRdfsForwardChainingSail fastRdfsForwardChainingSail, InferencerConnection e) {
        super(fastRdfsForwardChainingSail, e);
        this.fastRdfsForwardChainingSail = fastRdfsForwardChainingSail;
        this.connection = fastRdfsForwardChainingSail.data.getConnection();
    }

    public boolean isOpen() throws SailException {


        return connection.isOpen();
    }

    public void close() throws SailException {
        connection.close();


    }

    public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr, Dataset dataset, BindingSet bindingSet, boolean b) throws SailException {


        return connection.evaluate(tupleExpr, dataset, bindingSet, b);
    }

    public CloseableIteration<? extends Resource, SailException> getContextIDs() throws SailException {


        return connection.getContextIDs();
    }

    public CloseableIteration<? extends Statement, SailException> getStatements(Resource resource, IRI iri, Value value, boolean b, Resource... resources) throws SailException {


        return connection.getStatements(resource, iri, value, b, resources);
    }

    public long size(Resource... resources) throws SailException {


        return connection.size(resources);
    }

    protected Model createModel() {
        return null;
    }

    public void begin() throws SailException {
        connection.begin();


    }

    public void begin(IsolationLevel isolationLevel) throws UnknownSailTransactionStateException, SailException {
        connection.begin(isolationLevel);


    }

    public void flush() throws SailException {
        connection.flush();


    }

    public void prepare() throws SailException {
        connection.prepare();


    }

    public void commit() throws SailException {
        connection.commit();


    }

    public void rollback() throws SailException {
        connection.rollback();


    }

    protected void addAxiomStatements() throws SailException {

    }

    protected int applyRules(Model model) throws SailException {
        return 0;
    }

    public boolean isActive() throws UnknownSailTransactionStateException {


        return connection.isActive();
    }

    @Override
    public InferencerConnection getWrappedConnection() {
        return (InferencerConnection) connection;
    }


    public void addStatement(Resource subject, IRI predicate, Value object, Resource... resources) throws SailException {

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

        connection.addStatement(subject, predicate, object, resources);

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

    public void startUpdate(UpdateContext updateContext) throws SailException {
        throw new UnsupportedOperationException();


    }

    public void addStatement(UpdateContext updateContext, Resource resource, IRI iri, Value value, Resource... resources) throws SailException {
        throw new UnsupportedOperationException();


    }

    public void removeStatement(UpdateContext updateContext, Resource resource, IRI iri, Value value, Resource... resources) throws SailException {
        throw new UnsupportedOperationException();


    }

    public void endUpdate(UpdateContext updateContext) throws SailException {
        throw new UnsupportedOperationException();


    }

    public void clear(Resource... resources) throws SailException {


    }

    public CloseableIteration<? extends Namespace, SailException> getNamespaces() throws SailException {


        return null;
    }

    public String getNamespace(String s) throws SailException {


        return null;
    }

    public void setNamespace(String s, String s1) throws SailException {


    }

    public void removeNamespace(String s) throws SailException {


    }

    public void clearNamespaces() throws SailException {


    }
}
