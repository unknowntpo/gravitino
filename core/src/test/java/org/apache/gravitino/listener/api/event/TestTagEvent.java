/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.listener.api.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.TagEventDispatcher;
import org.apache.gravitino.listener.api.info.TagInfo;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestTagEvent {
  private TagEventDispatcher failureDispatcher;
  private TagEventDispatcher dispatcher;
  private DummyEventListener dummyEventListener;
  private Tag tag;

  @BeforeAll
  void init() {
    this.tag = mockTag();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    TagDispatcher tagExceptionDispatcher = mockExceptionTagDispatcher();
    this.failureDispatcher = new TagEventDispatcher(eventBus, tagExceptionDispatcher);
    TagDispatcher tagDispatcher = mockTagDispatcher();
    this.dispatcher = new TagEventDispatcher(eventBus, tagDispatcher);
  }

  @Test
  void testListTagsEvent() {
    dispatcher.listTags("metalake");
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals("metalake", Objects.requireNonNull(preEvent.identifier()).toString());
    Assertions.assertEquals(ListTagsPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_TAG, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals("metalake", Objects.requireNonNull(postevent.identifier()).toString());
    Assertions.assertEquals(ListTagsEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.LIST_TAG, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testListTagsInfo() {
    dispatcher.listTagsInfo("metalake");
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals("metalake", Objects.requireNonNull(preEvent.identifier()).toString());
    Assertions.assertEquals(ListTagsInfoPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_TAGS_INFO, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals("metalake", Objects.requireNonNull(postevent.identifier()).toString());
    Assertions.assertEquals(ListTagsInfoEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.LIST_TAGS_INFO, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testGetTag() {
    dispatcher.getTag("metalake", tag.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofTag("metalake", tag.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(GetTagPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_TAG, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();

    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(GetTagEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.GET_TAG, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
    TagInfo tagInfo = ((GetTagEvent) postevent).tagInfo();
    checkTagInfo(tagInfo, tag);
  }

  @Test
  void testCreateTag() {
    dispatcher.createTag("metalake", tag.name(), tag.comment(), tag.properties());
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofTag("metalake", tag.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(CreateTagPreEvent.class, preEvent.getClass());

    TagInfo tagInfo = ((CreateTagPreEvent) preEvent).tagInfo();
    checkTagInfo(tagInfo, tag);

    Assertions.assertEquals(OperationType.CREATE_TAG, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(CreateTagEvent.class, postevent.getClass());
    TagInfo tagInfo2 = ((CreateTagEvent) postevent).createdTagInfo();
    checkTagInfo(tagInfo2, tag);
    Assertions.assertEquals(OperationType.CREATE_TAG, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testAlterTag() {
    TagChange change1 = TagChange.rename("newName");
    TagChange[] changes = {change1};

    dispatcher.alterTag("metalake", tag.name(), changes);
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofTag("metalake", tag.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(AlterTagPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.ALTER_TAG, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    TagChange[] eventChanges = ((AlterTagPreEvent) preEvent).changes();
    Assertions.assertArrayEquals(changes, eventChanges);

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(AlterTagEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.ALTER_TAG, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());

    TagChange[] postChanges = ((AlterTagEvent) postevent).changes();
    Assertions.assertArrayEquals(changes, postChanges);
  }

  @Test
  void testDeleteTag() {
    dispatcher.deleteTag("metalake", tag.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofTag("metalake", tag.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(DeleteTagPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_TAG, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(DeleteTagEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.DELETE_TAG, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testListMetadataObjectsForTag() {
    dispatcher.listMetadataObjectsForTag("metalake", tag.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofTag("metalake", tag.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(ListMetadataObjectsForTagPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_METADATA_OBJECTS_FOR_TAG, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(ListMetadataObjectsForTagEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.LIST_METADATA_OBJECTS_FOR_TAG, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testListTagsForMetadataObject() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    dispatcher.listTagsForMetadataObject("metalake", metadataObject);
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent("metalake", metadataObject);

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(ListTagsForMetadataObjectPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_TAGS_FOR_METADATA_OBJECT, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(ListTagsForMetadataObjectEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.LIST_TAGS_FOR_METADATA_OBJECT, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testListTagsInfoForMetadataObject() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);
    dispatcher.listTagsInfoForMetadataObject("metalake", metadataObject);

    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent("metalake", metadataObject);

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(ListTagsInfoForMetadataObjectPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(
        OperationType.LIST_TAGS_INFO_FOR_METADATA_OBJECT, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(ListTagsInfoForMetadataObjectEvent.class, postevent.getClass());
    Assertions.assertEquals(
        OperationType.LIST_TAGS_INFO_FOR_METADATA_OBJECT, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testAssociateTagsForMetadataObjectPreEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    String[] tagsToAdd = {"tag1", "tag2"};
    String[] tagsToRemove = {"tag3"};

    dispatcher.associateTagsForMetadataObject("metalake", metadataObject, tagsToAdd, tagsToRemove);
    PreEvent preEvent = dummyEventListener.popPreEvent();

    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent("metalake", metadataObject);

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(AssociateTagsForMetadataObjectPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(
        MetadataObject.Type.CATALOG,
        ((AssociateTagsForMetadataObjectPreEvent) preEvent).objectType());
    Assertions.assertArrayEquals(
        tagsToAdd, ((AssociateTagsForMetadataObjectPreEvent) preEvent).tagsToAdd());
    Assertions.assertArrayEquals(
        tagsToRemove, ((AssociateTagsForMetadataObjectPreEvent) preEvent).tagsToRemove());

    Assertions.assertEquals(
        OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(AssociateTagsForMetadataObjectEvent.class, postevent.getClass());
    Assertions.assertEquals(
        MetadataObject.Type.CATALOG,
        ((AssociateTagsForMetadataObjectEvent) postevent).objectType());
    Assertions.assertArrayEquals(
        tagsToAdd, ((AssociateTagsForMetadataObjectEvent) postevent).tagsToAdd());
    Assertions.assertArrayEquals(
        tagsToRemove, ((AssociateTagsForMetadataObjectEvent) postevent).tagsToRemove());
    Assertions.assertEquals(
        OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testGetTagForMetadataObject() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    dispatcher.getTagForMetadataObject("metalake", metadataObject, tag.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();

    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent("metalake", metadataObject);

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(GetTagForMetadataObjectPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(tag.name(), ((GetTagForMetadataObjectPreEvent) preEvent).tagName());
    Assertions.assertEquals(OperationType.GET_TAG_FOR_METADATA_OBJECT, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(GetTagForMetadataObjectEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.GET_TAG_FOR_METADATA_OBJECT, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());

    TagInfo tagInfo = ((GetTagForMetadataObjectEvent) postevent).tagInfo();
    checkTagInfo(tagInfo, tag);
  }

  @Test
  void testCreateTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.createTag("metalake", tag.name(), tag.comment(), tag.properties()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(CreateTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((CreateTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals(tag.name(), ((CreateTagFailureEvent) event).tagInfo().name());
    Assertions.assertEquals(tag.comment(), ((CreateTagFailureEvent) event).tagInfo().comment());
    Assertions.assertEquals(
        tag.properties(), ((CreateTagFailureEvent) event).tagInfo().properties());
    Assertions.assertEquals(OperationType.CREATE_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.getTag("metalake", tag.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((GetTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetTagForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getTagForMetadataObject("metalake", metadataObject, tag.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetTagForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((GetTagForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_TAG_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDeleteTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.deleteTag("metalake", tag.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DeleteTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DELETE_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAlterTagFailureEvent() {
    TagChange change1 = TagChange.rename("newName");
    TagChange change2 = TagChange.updateComment("new comment");
    TagChange change3 = TagChange.setProperty("key", "value");
    TagChange change4 = TagChange.removeProperty("oldKey");
    TagChange[] changes = new TagChange[] {change1, change2, change3, change4};
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterTag("metalake", tag.name(), changes));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AlterTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals(changes, ((AlterTagFailureEvent) event).changes());
    Assertions.assertEquals(OperationType.ALTER_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listTags("metalake"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListTagsFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagsForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listTagsForMetadataObject("metalake", metadataObject));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListTagsForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_TAGS_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagsInfoFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listTagsInfo("metalake"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsInfoFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListTagsInfoFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_TAGS_INFO, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagsInfoForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listTagsInfoForMetadataObject("metalake", metadataObject));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsInfoForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListTagsInfoForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        OperationType.LIST_TAGS_INFO_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListMetadataObjectsForTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listMetadataObjectsForTag("metalake", tag.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListMetadataObjectsForTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListMetadataObjectsForTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_METADATA_OBJECTS_FOR_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAssociateTagsForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    String[] tagsToAssociate = new String[] {"tag1", "tag2"};
    String[] tagsToDisassociate = new String[] {"tag3", "tag4"};

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.associateTagsForMetadataObject(
                "metalake", metadataObject, tagsToAssociate, tagsToDisassociate));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AssociateTagsForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((AssociateTagsForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        MetadataObject.Type.CATALOG,
        ((AssociateTagsForMetadataObjectFailureEvent) event).objectType());
    Assertions.assertEquals(
        tagsToAssociate, ((AssociateTagsForMetadataObjectFailureEvent) event).tagsToAdd());
    Assertions.assertEquals(
        tagsToDisassociate, ((AssociateTagsForMetadataObjectFailureEvent) event).tagsToRemove());
    Assertions.assertEquals(
        OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  private Tag mockTag() {
    Tag tag = mock(Tag.class);
    when(tag.name()).thenReturn("tag");
    when(tag.comment()).thenReturn("comment");
    when(tag.properties()).thenReturn(ImmutableMap.of("color", "#FFFFFF"));
    return tag;
  }

  private TagDispatcher mockExceptionTagDispatcher() {
    return mock(
        TagDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }

  private void checkTagInfo(TagInfo actualTagInfo, Tag expectedTag) {
    Assertions.assertEquals(expectedTag.name(), actualTagInfo.name());
    Assertions.assertEquals(expectedTag.comment(), actualTagInfo.comment());
    Assertions.assertEquals(expectedTag.properties(), actualTagInfo.properties());
  }

  private TagDispatcher mockTagDispatcher() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    String metalake = "metalake";
    String[] tagNames = new String[] {"tag1", "tag2"};
    Tag[] tags = new Tag[] {tag, tag};

    when(dispatcher.createTag(
            any(String.class), any(String.class), any(String.class), any(Map.class)))
        .thenReturn(tag);
    when(dispatcher.listTags(metalake)).thenReturn(tagNames);
    when(dispatcher.listTagsInfo(metalake)).thenReturn(tags);
    when(dispatcher.alterTag(any(String.class), any(String.class), any(TagChange[].class)))
        .thenReturn(tag);
    when(dispatcher.getTag(any(String.class), any(String.class))).thenReturn(tag);
    when(dispatcher.deleteTag(metalake, tag.name())).thenReturn(true);
    when(dispatcher.getTagForMetadataObject(
            any(String.class), any(MetadataObject.class), any(String.class)))
        .thenReturn(tag);
    MetadataObject catalog =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);
    MetadataObject[] objects = new MetadataObject[] {catalog};

    when(dispatcher.listMetadataObjectsForTag(any(String.class), any(String.class)))
        .thenReturn(objects);

    when(dispatcher.associateTagsForMetadataObject(
            any(String.class), any(MetadataObject.class), any(String[].class), any(String[].class)))
        .thenReturn(new String[] {"tag1", "tag2"});

    when(dispatcher.listTagsForMetadataObject(any(String.class), any(MetadataObject.class)))
        .thenReturn(new String[] {"tag1", "tag2"});

    when(dispatcher.listTagsInfoForMetadataObject(any(String.class), any(MetadataObject.class)))
        .thenReturn(new Tag[] {tag, tag});

    return dispatcher;
  }
}
