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
import org.openrdf.IsolationLevels;
import org.openrdf.model.IRI;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.SimpleValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.*;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.AbstractNotifyingSail;
import org.openrdf.sail.inferencer.InferencerConnection;
import org.openrdf.sail.inferencer.fc.AbstractForwardChainingInferencer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class FastRdfsForwardChainingSail extends AbstractForwardChainingInferencer {

    final AbstractNotifyingSail data;
    final Repository schema;

    boolean sesameCompliant = false;

    List<Statement> subClassOfStatemenets = new ArrayList<>();
    List<Statement> propertyStatements = new ArrayList<>();
    List<Statement> subPropertyOfStatemenets = new ArrayList<>();
    List<Statement> rangeStatemenets = new ArrayList<>();
    List<Statement> domainStatemenets = new ArrayList<>();


    Map<IRI, HashSet<IRI>> calculatedTypes = new HashMap<>();
    Map<IRI, HashSet<IRI>> calculatedProperties = new HashMap<>();
    Map<IRI, HashSet<IRI>> calculatedRange = new HashMap<>();
    Map<IRI, HashSet<IRI>> calculatedDomain = new HashMap<>();


    void clearInferenceTables() {
        subClassOfStatemenets = new ArrayList<>();
        propertyStatements = new ArrayList<>();
        subPropertyOfStatemenets = new ArrayList<>();
        rangeStatemenets = new ArrayList<>();
        domainStatemenets = new ArrayList<>();
        calculatedTypes = new HashMap<>();
        calculatedProperties = new HashMap<>();
        calculatedRange = new HashMap<>();
        calculatedDomain = new HashMap<>();


    }


    public FastRdfsForwardChainingSail(AbstractNotifyingSail data) {
        super(data);
        schema = null;
        this.data = data;

    }

    public FastRdfsForwardChainingSail(AbstractNotifyingSail data, Repository schema) {
        super(data);

        this.data = data;
        this.schema = schema;

    }

    public FastRdfsForwardChainingSail(AbstractNotifyingSail data, boolean sesameCompliant) {
        super(data);
        schema = null;

        this.data = data;
        this.sesameCompliant = sesameCompliant;

    }

    public FastRdfsForwardChainingSail(AbstractNotifyingSail data, Repository schema, boolean sesameCompliant) {
        super(data);

        this.data = data;
        this.schema = schema;
        this.sesameCompliant = sesameCompliant;

    }


    public void initialize() throws SailException {
        super.initialize();

        FastRdfsForwardChainingSailConnetion connection = getConnection();
        connection.begin();


        List<Statement> schemaStatements = new ArrayList<>();


        if (schema != null) {

            try (RepositoryConnection schemaConnection = schema.getConnection()) {
                schemaConnection.begin();
                RepositoryResult<Statement> statements = schemaConnection.getStatements(null, null, null);

                schemaStatements = Iterations.stream(statements)
                    .peek(connection::statementCollector)
                    .collect(Collectors.toList());

                schemaConnection.commit();
            }

            iterateOverSchema(connection);
        }


        connection.temp();

        schemaStatements.forEach(s -> connection.addStatement(s.getSubject(), s.getPredicate(), s.getObject(), s.getContext()));

        connection.commit();
        connection.close();

    }

    private void iterateOverSchema(FastRdfsForwardChainingSailConnetion connection) {
        try {


            RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
            parser.setRDFHandler(new RDFHandler() {
                @Override
                public void startRDF() throws RDFHandlerException {

                }

                @Override
                public void endRDF() throws RDFHandlerException {

                }

                @Override
                public void handleNamespace(String s11, String s1) throws RDFHandlerException {

                }

                @Override
                public void handleStatement(Statement statement) throws RDFHandlerException {
                    connection.statementCollector(statement);
                    connection.addInferredStatement(statement.getSubject(), statement.getPredicate(), statement.getObject());
                }

                @Override
                public void handleComment(String s1) throws RDFHandlerException {

                }
            });

            parser.parse(new ByteArrayInputStream(baseRDFS.getBytes("UTF-8")), "");


        } catch (IOException ignored) {
        }
    }


    public void setDataDir(File file) {
        throw new UnsupportedOperationException();
    }

    public File getDataDir() {
        throw new UnsupportedOperationException();
    }


    public FastRdfsForwardChainingSailConnetion getConnection() throws SailException {
        InferencerConnection e = (InferencerConnection) super.getConnection();
        return new FastRdfsForwardChainingSailConnetion(this, e);
    }

    public ValueFactory getValueFactory() {

        return SimpleValueFactory.getInstance();
    }

    public List<IsolationLevel> getSupportedIsolationLevels() {
        ArrayList<IsolationLevel> isolationLevels = new ArrayList<>();
        isolationLevels.add(IsolationLevels.NONE);

        return isolationLevels;
    }

    public IsolationLevel getDefaultIsolationLevel() {
        return IsolationLevels.NONE;
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
