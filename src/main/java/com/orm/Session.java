package com.orm;

import com.orm.annotations.Column;
import com.orm.annotations.Id;
import com.orm.annotations.Table;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class Session {

    private static final String SELECT_FROM_TABLE_BY_ID = "select * from %s where id = ?";
    private static final String UPDATE_TEMPLATE = "UPDATE %s SET %s WHERE id = ?";

    private final DataSource dataSource;

    private Map<EntityKey<?>, Object> idToEntityCache = new HashMap<>();
    private Map<EntityKey<?>, Object[]> entitiesSnapshots = new HashMap<>();

    @SneakyThrows
    public <T> T find(Class<T> type, Object id) {
        var key = new EntityKey<>(type, id);

        var entity = idToEntityCache.computeIfAbsent(key, this::loadFromDB);

        return type.cast(entity);
    }

    @SneakyThrows
    private <T> T loadFromDB(EntityKey<T> key) {
        var id = key.getId();
        var type = key.getType();
        try (var connection = dataSource.getConnection()) {
            var selectQuery = prepareSelectQuery(type);
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
        final Field[] sortedDeclaredFields = Arrays.stream(entityType.getDeclaredFields())
            .sorted(Comparator.comparing(Field::getName)).toArray(Field[]::new);

        var snapshotCopy = new Object[sortedDeclaredFields.length];

        for (int i = 0; i < sortedDeclaredFields.length; i++) {

            var field = sortedDeclaredFields[i];

            var columnAnnotation = field.getDeclaredAnnotation(Column.class);
            var columnName = columnAnnotation.value();
            field.setAccessible(true);
            var fieldValue = resultSet.getObject(columnName);
            field.set(entity, fieldValue);
            snapshotCopy[i] = fieldValue;
        }

        entitiesSnapshots.put(entityKey, snapshotCopy);
        return entity;
    }

    private String prepareSelectQuery(Class<?> type) {
        var tableAnnotation = type.getDeclaredAnnotation(Table.class);
        return String.format(SELECT_FROM_TABLE_BY_ID, tableAnnotation.value());
    }

    @SneakyThrows
    private String prepareUpdateQuery(Map.Entry<EntityKey<?>, Object[]> entityKeyEntry) {
        final EntityKey<?> key = entityKeyEntry.getKey();
        var type = key.getType();

        var tableAnnotation = type.getDeclaredAnnotation(Table.class);
        StringBuilder stringBuilder = new StringBuilder();
        for (Field field : type.getDeclaredFields()) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(Id.class)) {
                continue;
            }

            String columnName = field.getDeclaredAnnotation(Column.class).value();
            Object newValue = field.get(idToEntityCache.get(key));

            stringBuilder.append(String.format("%s = '%s', ", columnName, newValue));
        }

        String newColumnValues = stringBuilder.toString();

        String newColumnValuesTrimmed = newColumnValues.substring(0, newColumnValues.length() - 2);
        String updateQuery = String.format(UPDATE_TEMPLATE, tableAnnotation.value(), newColumnValuesTrimmed);
        return updateQuery;
    }

    public void close() {
        //todo: compare entities with initial copies and perform update if needed
        entitiesSnapshots.entrySet()
            .stream()
            .filter(this::hasChanged)
            .forEach(this::performUpdate);
    }

    @SneakyThrows
    private void performUpdate(Map.Entry<EntityKey<?>, Object[]> entityKeyEntry) {
        try (var connection = dataSource.getConnection()) {
            var updateQuery = prepareUpdateQuery(entityKeyEntry);
            try (PreparedStatement ps = connection.prepareStatement(prepareUpdateQuery(entityKeyEntry))) {
                ps.setObject(1, entityKeyEntry.getKey().getId());
                System.out.println("SQL: " + updateQuery);
                ps.executeUpdate();
            }
        }
    }

    @SneakyThrows
    private boolean hasChanged(Map.Entry<EntityKey<?>, Object[]> entityEntry) {
        final Object cachedEntity = idToEntityCache.get(entityEntry.getKey());

        Class type = entityEntry.getKey().getType();
        final Field[] sortedDeclaredFields = Arrays.stream(type.getDeclaredFields())
            .sorted(Comparator.comparing(Field::getName)).toArray(Field[]::new);

        final Object[] initialFieldsValues = entityEntry.getValue(); //already sorted
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
