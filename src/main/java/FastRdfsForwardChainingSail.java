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


import info.aduna.iteration.Iterations;
import org.openrdf.IsolationLevel;
import org.openrdf.model.IRI;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.SimpleValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.*;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.AbstractNotifyingSail;
import org.openrdf.sail.inferencer.InferencerConnection;
import org.openrdf.sail.inferencer.fc.AbstractForwardChainingInferencer;
import org.openrdf.sail.inferencer.fc.AbstractForwardChainingInferencerConnection;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;

public class FastRdfsForwardChainingSail extends AbstractForwardChainingInferencer {

    final AbstractNotifyingSail data;
    private final Repository schema;
    private Map<IRI, HashSet<IRI>> calculatedTypes = new HashMap<>();
    private Map<IRI, HashSet<IRI>> calculatedProperties = new HashMap<>();
    private Map<IRI, HashSet<IRI>> calculatedRange = new HashMap<>();
    private Map<IRI, HashSet<IRI>> calculatedDomain = new HashMap<>();
    boolean sesameCompliant = false;


    public FastRdfsForwardChainingSail(AbstractNotifyingSail data, Repository schema) {
        super(data);

        this.data = data;
        this.schema = schema;

    }

    public FastRdfsForwardChainingSail(AbstractNotifyingSail data, Repository schema, boolean sesameCompliant) {
        super(data);

        this.data = data;
        this.schema = schema;
        this.sesameCompliant = sesameCompliant;

    }

    public void initialize() throws SailException {
        data.initialize();


        List<Statement> schemaStatements = null;

        try (RepositoryConnection schemaConnection = schema.getConnection()) {
            schemaConnection.begin();
            RepositoryResult<Statement> statements = schemaConnection.getStatements(null, null, null);

            schemaStatements = Iterations.stream(statements).collect(Collectors.toList());

            schemaConnection.commit();
        }

        addBaseRdfs(schema);


        calculateSubClassOf(schema);
        findProperties(schema);
        calculateSubPropertyOf(schema);

        calculateRangeDomain(schema, RDFS.RANGE, calculatedRange);
        calculateRangeDomain(schema, RDFS.DOMAIN, calculatedDomain);


        AbstractForwardChainingInferencerConnection connection = getConnection();
        connection.begin();
        calculatedTypes.forEach((subClass, superClasses) -> {
            connection.addInferredStatement(subClass, RDFS.SUBCLASSOF, subClass);

            superClasses.forEach(superClass -> {
                connection.addInferredStatement(subClass, RDFS.SUBCLASSOF, superClass);
                connection.addInferredStatement(superClass, RDFS.SUBCLASSOF, superClass);

            });
        });

        calculatedProperties.forEach((sub, sups) -> {
            connection.addInferredStatement(sub, RDFS.SUBPROPERTYOF, sub);

            sups.forEach(sup -> {
                connection.addInferredStatement(sub, RDFS.SUBPROPERTYOF, sup);
                connection.addInferredStatement(sup, RDFS.SUBPROPERTYOF, sup);

            });
        });


        schemaStatements.forEach(s -> {
            connection.addStatement(s.getSubject(), s.getPredicate(), s.getObject(), s.getContext());
        });

        connection.commit();
        connection.close();


        System.out.println("");


    }


    private void addBaseRdfs(Repository schema) {
        try (RepositoryConnection connection = schema.getConnection()) {
            AbstractForwardChainingInferencerConnection inferencerConnection = getConnection();
            inferencerConnection.begin();

            RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
            parser.setRDFHandler(new RDFHandler() {
                @Override
                public void startRDF() throws RDFHandlerException {

                }

                @Override
                public void endRDF() throws RDFHandlerException {

                }

                @Override
                public void handleNamespace(String s, String s1) throws RDFHandlerException {

                }

                @Override
                public void handleStatement(Statement statement) throws RDFHandlerException {
                    connection.add(statement);
                    inferencerConnection.addInferredStatement(statement.getSubject(), statement.getPredicate(), statement.getObject());
                }

                @Override
                public void handleComment(String s) throws RDFHandlerException {

                }
            });

            parser.parse(new ByteArrayInputStream(baseRDFS.getBytes("UTF-8")), "");

            inferencerConnection.commit();
            inferencerConnection.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void calculateSubClassOf(Repository schema) {
        try (RepositoryConnection connection = schema.getConnection()) {
            RepositoryResult<Statement> statements = connection.getStatements(null, RDFS.SUBCLASSOF, null);
            Iterations.stream(statements)
                .forEach(s -> {
                    IRI subClass = (IRI) s.getSubject();
                    if (!calculatedTypes.containsKey(subClass)) {
                        calculatedTypes.put(subClass, new HashSet<>());
                    }

                    calculatedTypes.get(subClass).add((IRI) s.getObject());

                });
        }

        long prevSize = 0;
        final long[] newSize = {-1};
        while (prevSize != newSize[0]) {

            prevSize = newSize[0];

            newSize[0] = 0;

            calculatedTypes.forEach((key, value) -> {
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

    private void findProperties(Repository schema) {
        try (RepositoryConnection connection = schema.getConnection()) {


            RepositoryResult<Statement> statements = connection.getStatements(null, RDF.TYPE, RDF.PROPERTY);
            Iterations.stream(statements)
                .forEach(s -> {
                    IRI property = (IRI) s.getSubject();

                    if (!calculatedProperties.containsKey(property)) {
                        calculatedProperties.put(property, new HashSet<>());
                    }

                });
        }

    }


    private void calculateSubPropertyOf(Repository schema) {
        try (RepositoryConnection connection = schema.getConnection()) {


            RepositoryResult<Statement> statements = connection.getStatements(null, RDFS.SUBPROPERTYOF, null);
            Iterations.stream(statements)
                .forEach(s -> {
                    IRI subClass = (IRI) s.getSubject();
                    IRI superClass = (IRI) s.getObject();
                    if (!calculatedProperties.containsKey(subClass)) {
                        calculatedProperties.put(subClass, new HashSet<>());
                    }

                    if (!calculatedProperties.containsKey(superClass)) {
                        calculatedProperties.put(superClass, new HashSet<>());
                    }

                    calculatedProperties.get(subClass).add((IRI) s.getObject());

                });
        }

        long prevSize = 0;
        final long[] newSize = {-1};
        while (prevSize != newSize[0]) {

            prevSize = newSize[0];

            newSize[0] = 0;

            calculatedProperties.forEach((key, value) -> {
                List<IRI> temp = new ArrayList<IRI>();
                value.forEach(superProperty -> {
                    temp
                        .addAll(resolveProperties(superProperty));
                });

                value.addAll(temp);
                newSize[0] += value.size();
            });


        }
    }

    private void calculateRangeDomain(Repository schema, IRI rangeOrDomain, Map<IRI, HashSet<IRI>> calculatedRangeOrDomain) {
        try (RepositoryConnection connection = schema.getConnection()) {
            RepositoryResult<Statement> statements = connection.getStatements(null, rangeOrDomain, null);
            Iterations.stream(statements)
                .forEach(s -> {
                    IRI predicate = (IRI) s.getSubject();
                    if (!calculatedProperties.containsKey(predicate)) {
                        calculatedProperties.put(predicate, new HashSet<>());
                    }

                    if (!calculatedRangeOrDomain.containsKey(predicate)) {
                        calculatedRangeOrDomain.put(predicate, new HashSet<>());
                    }

                    calculatedRangeOrDomain.get(predicate).add((IRI) s.getObject());

                    if (!calculatedTypes.containsKey(s.getObject())) {
                        calculatedTypes.put((IRI) s.getObject(), new HashSet<>());
                    }

                });
        }

        calculatedProperties
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


    public void setDataDir(File file) {
        throw new UnsupportedOperationException();
    }

    public File getDataDir() {
        throw new UnsupportedOperationException();
    }


    public void shutDown() throws SailException {

    }

    public boolean isWritable() throws SailException {
        return false;
    }

    public AbstractForwardChainingInferencerConnection getConnection() throws SailException {
        InferencerConnection e = (InferencerConnection) super.getConnection();
        return new FastRdfsForwardChainingSailConnetion(this, e);
    }

    public ValueFactory getValueFactory() {

        return SimpleValueFactory.getInstance();
    }

    public List<IsolationLevel> getSupportedIsolationLevels() {
        return null;
    }

    public IsolationLevel getDefaultIsolationLevel() {
        return null;
    }

    Set<IRI> resolveTypes(IRI value) {
        Set<IRI> iris = calculatedTypes.get(value);

        return iris != null ? iris : Collections.emptySet();
    }

    Set<IRI> resolveProperties(IRI predicate) {
        Set<IRI> iris = calculatedProperties.get(predicate);

        return iris != null ? iris : Collections.emptySet();
    }


    Set<IRI> resolveRangeTypes(IRI predicate) {
        Set<IRI> iris = calculatedRange.get(predicate);

        return iris != null ? iris : Collections.emptySet();
    }

    Set<IRI> resolveDomainTypes(IRI predicate) {
        Set<IRI> iris = calculatedDomain.get(predicate);

        return iris != null ? iris : Collections.emptySet();
    }


    static final String baseRDFS =
        "@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
            "@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .\n" +
            "@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .\n" +
            "\n" +
            "rdf:Alt  a               rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdfs:Container , rdf:Alt .\n" +
            "\n" +
            "rdf:Bag  a               rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdfs:Container , rdf:Bag .\n" +
            "\n" +
            "rdf:List  a              rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdf:List .\n" +
            "\n" +
            "rdf:Property  a          rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdf:Property .\n" +
            "\n" +
            "rdf:Seq  a               rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdfs:Container , rdf:Seq .\n" +
            "\n" +
            "\n" +
            "rdf:Statement  a         rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdf:Statement .\n" +
            "\n" +
            "rdf:XMLLiteral  a        rdfs:Resource , rdfs:Datatype , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdfs:Literal , rdf:XMLLiteral .\n" +
            "\n" +
            "rdf:first  a                rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdf:List ;\n" +
            "        rdfs:range          rdfs:Resource ;\n" +
            "        rdfs:subPropertyOf  rdf:first .\n" +
            "\n" +
            "rdf:nil  a      rdfs:Resource , rdf:List .\n" +
            "\n" +
            "rdf:object  a               rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdf:Statement ;\n" +
            "        rdfs:range          rdfs:Resource ;\n" +
            "        rdfs:subPropertyOf  rdf:object .\n" +
            "\n" +
            "rdf:predicate  a            rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdf:Statement ;\n" +
            "        rdfs:range          rdfs:Resource ;\n" +
            "        rdfs:subPropertyOf  rdf:predicate .\n" +
            "\n" +
            "rdf:rest  a                 rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdf:List ;\n" +
            "        rdfs:range          rdf:List ;\n" +
            "        rdfs:subPropertyOf  rdf:rest .\n" +
            "\n" +
            "rdf:subject  a              rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdf:Statement ;\n" +
            "        rdfs:range          rdfs:Resource ;\n" +
            "        rdfs:subPropertyOf  rdf:subject .\n" +
            "\n" +
            "rdf:type  a                 rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdfs:Resource ;\n" +
            "        rdfs:range          rdfs:Class ;\n" +
            "        rdfs:subPropertyOf  rdf:type .\n" +
            "\n" +
            "rdf:value  a                rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdfs:Resource ;\n" +
            "        rdfs:range          rdfs:Resource ;\n" +
            "        rdfs:subPropertyOf  rdf:value .\n" +
            "\n" +
            "rdfs:Class  a            rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdfs:Class .\n" +
            "\n" +
            "rdfs:Container  a        rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdfs:Container .\n" +
            "\n" +
            "rdfs:ContainerMembershipProperty\n" +
            "        a                rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdfs:ContainerMembershipProperty , rdf:Property .\n" +
            "\n" +
            "rdfs:Datatype  a         rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdfs:Datatype , rdfs:Class .\n" +
            "\n" +
            "rdfs:Literal  a          rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource , rdfs:Literal .\n" +
            "\n" +
            "rdfs:Resource  a         rdfs:Resource , rdfs:Class ;\n" +
            "        rdfs:subClassOf  rdfs:Resource .\n" +
            "\n" +
            "rdfs:comment  a             rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdfs:Resource ;\n" +
            "        rdfs:range          rdfs:Literal ;\n" +
            "        rdfs:subPropertyOf  rdfs:comment .\n" +
            "\n" +
            "rdfs:domain  a              rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdf:Property ;\n" +
            "        rdfs:range          rdfs:Class ;\n" +
            "        rdfs:subPropertyOf  rdfs:domain .\n" +
            "\n" +
            "rdfs:isDefinedBy  a         rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdfs:Resource ;\n" +
            "        rdfs:range          rdfs:Resource ;\n" +
            "        rdfs:subPropertyOf  rdfs:seeAlso , rdfs:isDefinedBy .\n" +
            "\n" +
            "rdfs:label  a               rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdfs:Resource ;\n" +
            "        rdfs:range          rdfs:Literal ;\n" +
            "        rdfs:subPropertyOf  rdfs:label .\n" +
            "\n" +
            "rdfs:member  a              rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdfs:Resource ;\n" +
            "        rdfs:range          rdfs:Resource ;\n" +
            "        rdfs:subPropertyOf  rdfs:member .\n" +
            "\n" +
            "rdfs:range  a               rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdf:Property ;\n" +
            "        rdfs:range          rdfs:Class ;\n" +
            "        rdfs:subPropertyOf  rdfs:range .\n" +
            "\n" +
            "rdfs:seeAlso  a             rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdfs:Resource ;\n" +
            "        rdfs:range          rdfs:Resource ;\n" +
            "        rdfs:subPropertyOf  rdfs:seeAlso .\n" +
            "\n" +
            "rdfs:subClassOf  a          rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdfs:Class ;\n" +
            "        rdfs:range          rdfs:Class ;\n" +
            "        rdfs:subPropertyOf  rdfs:subClassOf .\n" +
            "\n" +
            "rdfs:subPropertyOf  a       rdfs:Resource , rdf:Property ;\n" +
            "        rdfs:domain         rdf:Property ;\n" +
            "        rdfs:range          rdf:Property ;\n" +
            "        rdfs:subPropertyOf  rdfs:subPropertyOf .";


}
