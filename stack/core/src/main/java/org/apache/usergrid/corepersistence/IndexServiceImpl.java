/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.usergrid.corepersistence;


import java.util.Collection;

import org.apache.usergrid.persistence.collection.serialization.SerializationFig;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.entities.Application;
import org.apache.usergrid.persistence.graph.Edge;
import org.apache.usergrid.persistence.graph.GraphManager;
import org.apache.usergrid.persistence.graph.GraphManagerFactory;
import org.apache.usergrid.persistence.graph.serialization.EdgesObservable;
import org.apache.usergrid.persistence.index.ApplicationEntityIndex;
import org.apache.usergrid.persistence.index.EntityIndexFactory;
import org.apache.usergrid.persistence.index.IndexEdge;
import org.apache.usergrid.persistence.index.impl.IndexIdentifierImpl;
import org.apache.usergrid.persistence.model.entity.Entity;
import org.apache.usergrid.persistence.model.entity.Id;
import org.apache.usergrid.persistence.schema.CollectionInfo;
import org.apache.usergrid.utils.InflectionUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import rx.Observable;
import rx.functions.Func1;
import rx.observables.MathObservable;

import static org.apache.usergrid.corepersistence.util.CpNamingUtils.generateScopeFromSource;
import static org.apache.usergrid.corepersistence.util.CpNamingUtils.generateScopeToTarget;
import static org.apache.usergrid.persistence.Schema.getDefaultSchema;


/**
 * Implementation of the indexing service
 */
@Singleton
public class IndexServiceImpl implements IndexService {

    private final GraphManagerFactory graphManagerFactory;
    private final EntityIndexFactory entityIndexFactory;
    private final EdgesObservable edgesObservable;
    private final SerializationFig serializationFig;


    @Inject
    public IndexServiceImpl( final GraphManagerFactory graphManagerFactory, final EntityIndexFactory entityIndexFactory,
                             final EdgesObservable edgesObservable, final SerializationFig serializationFig ) {
        this.graphManagerFactory = graphManagerFactory;
        this.entityIndexFactory = entityIndexFactory;
        this.edgesObservable = edgesObservable;
        this.serializationFig = serializationFig;
    }


    @Override
    public Observable<Integer> indexEntity( final ApplicationScope applicationScope, final Entity entity ) {


        final GraphManager gm = graphManagerFactory.createEdgeManager( applicationScope );

        // loop through all types of edge to target


        final ApplicationEntityIndex ei = entityIndexFactory.createApplicationEntityIndex( applicationScope );


        //get all the source edges for an entity
        final Observable<Edge> edgesToTarget = edgesObservable.edgesToTarget( gm, entity.getId() );


        final Id entityId = entity.getId();

        final Observable<IndexEdge> targetIndexEdges = edgesToTarget.map( edge -> generateScopeToTarget( edge ) );



        //we might or might not need to index from target-> source


        final Observable<IndexEdge>
            targetSizes = getIndexEdgesToTarget( gm, ei, entity );


        final Observable<IndexIdentifierImpl.IndexOperationMessage> observable = Observable.merge( targetIndexEdges,
            targetSizes ).buffer( serializationFig.getBufferSize() ).flatMap( buffer ->
            Observable.from(buffer).collect( () -> ei.createBatch(), ( batch, indexEdge ) -> batch.index( indexEdge, entity ) ).flatMap( batch -> Observable.from( batch.execute() ) );





        final Observable<IndexIdentifierImpl.IndexOperationMessage> futures = Observable.merge()
        return MathObservable.sumInteger( sourceSizes );
    }


    /**
     * Get index edgs to the target
     * @param graphManager
     * @param ei The application entity index
     * @param entity The entity
     * @return
     */
    private Observable<IndexEdge> getIndexEdgesToTarget(
        final GraphManager graphManager, final ApplicationEntityIndex ei, final Entity entity  ) {

        final Id entityId = entity.getId();
        final String collectionName = InflectionUtils.pluralize( entityId.getType() );


        final CollectionInfo collection = getDefaultSchema().getCollection( Application.ENTITY_TYPE, collectionName );

        //nothing to do
        if ( collection == null ) {
            return Observable.empty();
        }


        final String linkedCollection = collection.getLinkedCollection();

        /**
         * Nothing to link
         */
        if ( linkedCollection == null ) {
            return Observable.empty();
        }


        /**
         * An observable of sizes as we execute batches
         */
       return edgesObservable.getEdgesFromSource( graphManager, entityId, linkedCollection ).map( edge -> generateScopeFromSource( edge ) );
    }


}
