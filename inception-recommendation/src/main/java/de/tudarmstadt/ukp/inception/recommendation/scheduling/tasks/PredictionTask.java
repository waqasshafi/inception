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
package de.tudarmstadt.ukp.inception.recommendation.scheduling.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.fit.util.CasUtil;
import org.apache.uima.jcas.JCas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import de.tudarmstadt.ukp.clarin.webanno.api.AnnotationSchemaService;
import de.tudarmstadt.ukp.clarin.webanno.api.DocumentService;
import de.tudarmstadt.ukp.clarin.webanno.model.AnnotationDocument;
import de.tudarmstadt.ukp.clarin.webanno.model.AnnotationFeature;
import de.tudarmstadt.ukp.clarin.webanno.model.AnnotationLayer;
import de.tudarmstadt.ukp.clarin.webanno.model.Project;
import de.tudarmstadt.ukp.clarin.webanno.security.model.User;
import de.tudarmstadt.ukp.inception.recommendation.api.ClassificationTool;
import de.tudarmstadt.ukp.inception.recommendation.api.Classifier;
import de.tudarmstadt.ukp.inception.recommendation.api.LearningRecordService;
import de.tudarmstadt.ukp.inception.recommendation.api.RecommendationService;
import de.tudarmstadt.ukp.inception.recommendation.api.model.AnnotationObject;
import de.tudarmstadt.ukp.inception.recommendation.api.model.LearningRecord;
import de.tudarmstadt.ukp.inception.recommendation.api.model.LearningRecordUserAction;
import de.tudarmstadt.ukp.inception.recommendation.api.model.Predictions;
import de.tudarmstadt.ukp.inception.recommendation.api.model.Recommender;

/**
 * This consumer predicts new annotations for a given annotation layer, if a classification tool for
 * this layer was selected previously.
 */
public class PredictionTask
    extends Task
{
    private Logger log = LoggerFactory.getLogger(getClass());
    
    private @Autowired AnnotationSchemaService annoService;
    private @Autowired RecommendationService recommendationService;
    private @Autowired DocumentService documentService;
    private @Autowired LearningRecordService learningRecordService;
    
    public PredictionTask(User aUser, Project aProject)
    {
        super(aProject, aUser);
    }

    @Override
    public void run()
    {
        User user = getUser();

        Predictions model = new Predictions(getProject(), getUser()); 

        for (AnnotationLayer layer : annoService.listAnnotationLayer(getProject())) {
            if (!layer.isEnabled()) {
                continue;
            }
            
            List<Recommender> recommenders = recommendationService.getActiveRecommenders(user,
                    layer);

            if (recommenders.isEmpty()) {
                log.debug("[{}][{}]: No active recommenders, skipping prediction.",
                        user.getUsername(), layer.getUiName());
                continue;
            }
            
            for (Recommender recommender : recommenders) {
                long startTime = System.currentTimeMillis();
                
                ClassificationTool<?> ct = recommendationService.getTool(recommender,
                        recommendationService.getMaxSuggestions(user));
                Classifier<?> classifier = ct.getClassifier();

                classifier.setUser(getUser());
                classifier.setProject(getProject());
                classifier.setModel(recommendationService.getTrainedModel(user, recommender));
    
                List<AnnotationObject> predictions = new ArrayList<>();
                
                log.info("[{}][{}]: Predicting labels...", user.getUsername(),
                        recommender.getName());
                
                List<AnnotationDocument> docs = documentService
                        .listAnnotationDocuments(layer.getProject(), user);

                // TODO Reading the CAS is time-intensive, better parallelize it.
                for (AnnotationDocument doc : docs) {
                    try {
                        JCas jcas = documentService.readAnnotationCas(doc);
                        List<List<AnnotationObject>> documentPredictions = setVisibility(
                            learningRecordService, annoService, jcas, getUser().getUsername(), doc,
                            layer, Predictions.getPredictions(classifier.predict(jcas, layer)), 0,
                            jcas.getDocumentText().length() - 1);
                        documentPredictions.forEach(predictions::addAll);
                    }
                    catch (IOException e) {
                        log.error("Cannot read annotation CAS.", e);
                    }
                }
      
                // Tell the predictions who created them
                predictions.forEach(token -> token.setRecommenderId(recommender.getId()));

                if (predictions.isEmpty()) {
                    log.info("[{}][{}]: No prediction data.", user.getUsername(),
                            recommender.getName());
                    continue;
                }
                
                model.putPredictions(layer.getId(), predictions);
                
                log.info("[{}][{}]: Prediction complete ({} ms)", user.getUsername(),
                        recommender.getName(), (System.currentTimeMillis() - startTime));
            }
        }
        
        recommendationService.putIncomingPredictions(getUser(), getProject(), model);
    }

    /**
     * Goes through all AnnotationObjects and determines the visibility of each one
     */
    public static List<List<AnnotationObject>> setVisibility(
        LearningRecordService aLearningRecordService, AnnotationSchemaService aAnnotationService,
        JCas aJcas, String aUser, AnnotationDocument aDoc, AnnotationLayer aLayer,
        List<List<AnnotationObject>> aRecommendations, int aWindowBegin, int aWindowEnd)
    {
        // No recommendations
        if (aRecommendations == null || aRecommendations.isEmpty()) {
            return Collections.emptyList();
        }

        List<LearningRecord> recordedAnnotations = aLearningRecordService
            .getAllRecordsByDocumentAndUserAndLayer(aDoc.getDocument(), aUser, aLayer);

        // Recommendations sorted by Offset, Id, RecommenderId, DocumentName.hashCode (descending)
        NavigableSet<AnnotationObject> remainingRecommendations = new TreeSet<>();
        aRecommendations.forEach(remainingRecommendations::addAll);

        Type type = CasUtil.getType(aJcas.getCas(), aLayer.getName());
        Collection<AnnotationFS> existingAnnotations = CasUtil.select(aJcas.getCas(), type)
            .stream()
            .filter(fs -> fs.getBegin() >= aWindowBegin && fs.getEnd() <= aWindowEnd)
            .collect(Collectors.toList());

        AnnotationObject swap = remainingRecommendations.pollFirst();

        for (AnnotationFeature feature: aAnnotationService.listAnnotationFeature(aLayer)) {

            // Keep only AnnotationFS that have at least one feature value set
            List<AnnotationFS> annoFsForFeature = existingAnnotations.stream()
                .filter(fs ->
                    fs.getStringValue(fs.getType().getFeatureByBaseName(feature.getName())) != null)
                .collect(Collectors.toList());
            for (AnnotationFS fs : annoFsForFeature) {

                AnnotationObject ao = swap;

                // Go to the next token for which an annotation exists
                while (ao.getOffset().getBeginCharacter() < fs.getBegin()
                    && !remainingRecommendations.isEmpty()) {

                    setVisibility(recordedAnnotations, ao);
                    ao = remainingRecommendations.pollFirst();
                    swap = ao;
                }

                // For tokens with annotations also check whether the annotation is for the same
                // feature as the predicted label
                while (ao.getOffset().getBeginCharacter() == fs.getBegin()
                    && !remainingRecommendations.isEmpty()) {

                    if (isOverlappingForFeature(fs, ao, feature)) {
                        ao.setVisible(false);
                    } else {
                        setVisibility(recordedAnnotations, ao);
                    }
                    ao = remainingRecommendations.pollFirst();
                    swap = ao;
                }
            }

            // Check last AnnotationObject
            if (swap != null && !annoFsForFeature.isEmpty()) {
                if (isOverlappingForFeature(annoFsForFeature.get(annoFsForFeature.size() - 1), swap, feature)) {
                    swap.setVisible(false);
                }
                else {
                    setVisibility(recordedAnnotations, swap);
                }
            }
        }

        // Check for the remaining AnnotationObjects whether they have an annotation
        // and are not rejected
        for (AnnotationObject ao: remainingRecommendations) {
            setVisibility(recordedAnnotations, ao);
        }

        return aRecommendations;
    }

    private static boolean isOverlappingForFeature(AnnotationFS aFs, AnnotationObject aAo,
        AnnotationFeature aFeature)
    {
        return aFeature.getName().equals(aAo.getFeature()) &&
            ((aFs.getBegin() <= aAo.getOffset().getBeginCharacter())
                && (aFs.getEnd() >= aAo.getOffset().getEndCharacter())
            || (aFs.getBegin() >= aAo.getOffset().getBeginCharacter())
                && (aFs.getEnd() <= aAo.getOffset().getEndCharacter())
            || (aFs.getBegin() >= aAo.getOffset().getBeginCharacter())
                && (aFs.getEnd() >= aAo.getOffset().getEndCharacter())
                && (aFs.getBegin() < aAo.getOffset().getEndCharacter())
            || (aFs.getBegin() <= aAo.getOffset().getBeginCharacter())
                && (aFs.getEnd() <= aAo.getOffset().getEndCharacter())
                && (aFs.getEnd() > aAo.getOffset().getBeginCharacter()));
    }

    /**
     * Determines whether this recommendation has been rejected
     */
    private static boolean isRejected(List<LearningRecord> aRecordedRecommendations,
        AnnotationObject aAo)
    {
        for (LearningRecord record : aRecordedRecommendations) {
            if (record.getOffsetCharacterBegin() == aAo.getOffset().getBeginCharacter()
                && record.getOffsetCharacterEnd() == aAo.getOffset().getEndCharacter()
                && record.getAnnotation().equals(aAo.getLabel())
                && record.getUserAction().equals(LearningRecordUserAction.REJECTED)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Sets visibility of an AnnotationObject based on label and Learning Record
     */
    private static boolean setVisibility(List<LearningRecord> aRecordedRecommendations,
        AnnotationObject aAo)
    {
        boolean hasNoAnnotation = aAo.getLabel() == null;
        boolean isRejected = isRejected(aRecordedRecommendations, aAo);
        if (hasNoAnnotation || isRejected) {
            aAo.setVisible(false);
            return false;
        }
        else {
            aAo.setVisible(true);
            return true;
        }
    }
}
