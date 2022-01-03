package com.orm;

import com.orm.annotations.Column;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

@RequiredArgsConstructor
public class Session {

    private final DataSource dataSource;

    private Map<EntityKey<?>, Object> idToEntityCache = new HashMap<>();
    private Map<EntityKey<?>, Object[]> snapshotCopies = new HashMap<>();

    @SneakyThrows
    public <T> T find(Class<T> type, Object id) {
        EntityKey<T> key = new EntityKey<>(type, id);

        if (idToEntityCache.containsKey(key)) {
            return type.cast(idToEntityCache.get(key));
        }

        var entity = loadFromDB(key);
        idToEntityCache.put(key, entity);
        createSnapshotCopy(key, entity);

        return type.cast(entity);
    }

    @SneakyThrows
    private void createSnapshotCopy(EntityKey<?> key, Object entity) {
        Field[] declaredFields = entity.getClass().getDeclaredFields();
        SortedMap<String, Object> fieldValues = new TreeMap<>();

        for (Field field : declaredFields) {
            field.setAccessible(true);
            fieldValues.put(field.getName(), field.get(entity));
        }

        snapshotCopies.put(key, fieldValues.values().toArray());
    }

    @SneakyThrows
    private <T> T loadFromDB(EntityKey<T> key) {
        var id = key.getId();
        var type = key.getType();
        try (var connection = dataSource.getConnection()) {
            var selectQuery = QueryBuilder.prepareSelectQuery(type);
            try (PreparedStatement statement = connection.prepareStatement(selectQuery)) {
                statement.setObject(1, id);
                System.out.println("SQL: " + selectQuery);
                final ResultSet resultSet = statement.executeQuery();
                return createEntityFromResult(key, resultSet);
            }
        }
    }

    @SneakyThrows
    private <T> T createEntityFromResult(EntityKey<T> entityKey, ResultSet resultSet) {
        var entityType = entityKey.getType();
        resultSet.next();

        var entity = entityType.getConstructor().newInstance();
        Field[] sortedDeclaredFields = Arrays.stream(entityType.getDeclaredFields())
            .sorted(Comparator.comparing(Field::getName)).toArray(Field[]::new);

        for (int i = 0; i < sortedDeclaredFields.length; i++) {

            var field = sortedDeclaredFields[i];

            var columnAnnotation = field.getDeclaredAnnotation(Column.class);
            var columnName = columnAnnotation.value();
            field.setAccessible(true);
            var fieldValue = resultSet.getObject(columnName);
            field.set(entity, fieldValue);
        }

        return entity;
    }

    public void close() {
        idToEntityCache.entrySet()
            .stream()
            .filter(this::hasChanged)
            .forEach(this::performUpdate);
    }

    @SneakyThrows
    private void performUpdate(Map.Entry<EntityKey<?>, Object> entityKeyEntry) {
        try (Connection connection = dataSource.getConnection()) {
            String preparedUpdateQuery = QueryBuilder.prepareUpdateQuery(entityKeyEntry.getValue());
            try (PreparedStatement statement = connection.prepareStatement(preparedUpdateQuery)) {
                statement.setObject(1, entityKeyEntry.getKey().getId());
                statement.executeUpdate();
            }
        }
    }

    @SneakyThrows
    private boolean hasChanged(Map.Entry<EntityKey<?>, Object> entityEntry) {
        Class<?> type = entityEntry.getKey().getType();
        Field[] sortedDeclaredFields = Arrays.stream(type.getDeclaredFields())
            .sorted(Comparator.comparing(Field::getName)).toArray(Field[]::new);

        Object[] initialFieldsValues = snapshotCopies.get(entityEntry.getKey()); //already sorted
        Object cachedEntity = entityEntry.getValue();
        for (int i = 0; i < sortedDeclaredFields.length; i++) {
            Field field = sortedDeclaredFields[i];
            field.setAccessible(true);

            Object actualValue = field.get(cachedEntity);
            Object initialValue = initialFieldsValues[i];
            if (!actualValue.equals(initialValue)) {
                return true;
            }
        }

        return false;
    }
}
