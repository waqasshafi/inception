/*
 * Copyright 2017
 * Ubiquitous Knowledge Processing (UKP) Lab
 * Technische Universität Darmstadt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.tudarmstadt.ukp.inception.kb.graph;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import de.tudarmstadt.ukp.inception.kb.model.KnowledgeBase;

public class KBConcept
    implements KBObject
{
    private static final long serialVersionUID = -7289788139970822316L;

    private String identifier;
    private String name;
    private String description;
    private KnowledgeBase kb;
    private String language;



    private List<Statement> originalStatements = new ArrayList<>();



    public KBConcept()
    {
        super();
    }

    public KBConcept(String aIdentifier, String aName)
    {
        identifier = aIdentifier;
        name = aName;
    }

    @Override
    public String getIdentifier()
    {
        return identifier;
    }

    @Override
    public void setIdentifier(String aIdentifier)
    {
        identifier = aIdentifier;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void setName(String aName)
    {
        name = aName;
    }

    @Override
    public KnowledgeBase getKB()
    {
        return kb;
    }

    @Override
    public void setKB(KnowledgeBase akb)
    {
        kb = akb;
    }


    public String getDescription()
    {
        return description;
    }

    public void setDescription(String aDescription)
    {
        description = aDescription;
    }

    @Override
    public String getLanguage()
    {
        return language;
    }

    @Override
    public void setLanguage(String aLanguage)
    {
        language = aLanguage;
    }

    public List<Statement> getOriginalStatements()
    {
        return originalStatements;
    }

    public void write(RepositoryConnection aConn, KnowledgeBase kb)
    {
        ValueFactory vf = aConn.getValueFactory();
        IRI subject = vf.createIRI(identifier);

        originalStatements.clear();

        Statement typeStmt = vf.createStatement(subject, kb.getTypeIri(), kb.getClassIri());
        originalStatements.add(typeStmt);
        aConn.add(typeStmt);

        if (isNotBlank(name)) {
            Literal nameLiteral;
            if (language != null) {
                nameLiteral = vf.createLiteral(name, language);
            }
            else if (kb.getDefaultLanguage() != null) {
                nameLiteral = vf.createLiteral(name, kb.getDefaultLanguage());
            }
            else {
                nameLiteral = vf.createLiteral(name);
            }
            Statement nameStmt = vf.createStatement(subject, kb.getLabelIri(), nameLiteral);
            originalStatements.add(nameStmt);
            aConn.add(nameStmt);
        }
        if (isNotBlank(description)) {
            Literal descriptionLiteral;
            if (language == null) {
                descriptionLiteral = vf.createLiteral(description);
            }
            else {
                descriptionLiteral = vf.createLiteral(description, language);
            }
            Statement descStmt = vf
                .createStatement(subject, kb.getDescriptionIri(), descriptionLiteral);
            originalStatements.add(descStmt);
            aConn.add(descStmt);
        }

    }
    
    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append("identifier", identifier).append("name", name)
                .append("language", language).append("description", description).toString();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KBConcept other = (KBConcept) obj;
        if (identifier == null) {
            if (other.identifier != null) {
                return false;
            }
        }
        else if (!identifier.equals(other.identifier)) {
            return false;
        }
        return true;
    }
}
