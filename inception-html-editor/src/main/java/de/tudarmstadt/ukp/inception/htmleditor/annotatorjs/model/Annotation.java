/*
 * Copyright 2017
 * Ubiquitous Knowledge Processing (UKP) Lab
 * Technische Universität Darmstadt
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.tudarmstadt.ukp.inception.htmleditor.annotatorjs.model;

import java.util.List;

public class Annotation
{
    private String id;
    private List<Range> ranges;
    private String quote;
    private String text;

    public String getId()
    {
        return id;
    }

    public void setId(String aId)
    {
        id = aId;
    }

    public List<Range> getRanges()
    {
        return ranges;
    }

    public void setRanges(List<Range> aRanges)
    {
        ranges = aRanges;
    }

    public String getQuote()
    {
        return quote;
    }

    public void setQuote(String aQuote)
    {
        quote = aQuote;
    }

    public String getText()
    {
        return text;
    }

    public void setText(String aText)
    {
        text = aText;
    }
}
