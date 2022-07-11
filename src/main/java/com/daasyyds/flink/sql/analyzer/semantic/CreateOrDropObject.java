package com.daasyyds.flink.sql.analyzer.semantic;

import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;

import java.util.Objects;

public class CreateOrDropObject {

    private IdentifierLike identifier;
    private ObjectType objectType;
    private boolean isTemporary;
    private ActionType actionType;

    private CreateOrDropObject() {}

    private CreateOrDropObject(IdentifierLike identifier, ObjectType objectType, boolean isTemporary, ActionType actionType) {
        this.identifier = identifier;
        this.objectType = objectType;
        this.isTemporary = isTemporary;
        this.actionType = actionType;
    }

    public static CreateOrDropObject of(IdentifierLike identifier, ObjectType type, boolean isTemporary, boolean isDrop) {
        return new CreateOrDropObject(identifier, type, isTemporary, isDrop ? ActionType.DROP : ActionType.CREATE);
    }

    public final CreateOrDropObject newTransient() {
        return new CreateOrDropObject(identifier, objectType, isTemporary, ActionType.TRANSIENT);
    }

    public boolean isCreated() {
        return actionType.equals(ActionType.CREATE);
    }

    public boolean isDropped() {
        return actionType.equals(ActionType.DROP);
    }

    public boolean isTransient() {
        return actionType.equals(ActionType.TRANSIENT);
    }

    public boolean matchObject(IdentifierLike il) {
        return this.identifier.equals(il);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CreateOrDropObject that = (CreateOrDropObject) o;
        return isTemporary == that.isTemporary &&
                identifier.equals(that.identifier) &&
                objectType == that.objectType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, objectType, isTemporary);
    }

    public IdentifierLike getIdentifier() {
        return identifier;
    }

    public ObjectType getObjectType() {
        return objectType;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public ActionType getActionType() {
        return actionType;
    }

    public enum ObjectType {
        TABLE,
        VIEW,
        FUNCTION
    }

    public enum ActionType {
        CREATE,
        DROP,
        TRANSIENT
    }
}
