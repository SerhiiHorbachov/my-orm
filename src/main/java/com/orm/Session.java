package com.orm;

import com.orm.annotations.Column;
import com.orm.annotations.Table;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class Session {

    private static final String SELECT_FROM_TABLE_BY_ID = "select * from %s where id = ?";

    private final DataSource dataSource;

    private Map<EntityKey<?>, Object> idToEntityCache = new HashMap<>();

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
                return createEntityFromResult(resultSet, type);
            }
        }
    }

    @SneakyThrows
    private <T> T createEntityFromResult(ResultSet resultSet, Class<T> type) {
        resultSet.next();
        var entity = type.getConstructor().newInstance();

        for (var field : type.getDeclaredFields()) {
            var columnAnnotation = field.getDeclaredAnnotation(Column.class);
            var columnName = columnAnnotation.value();
            field.setAccessible(true);
            var fieldValue = resultSet.getObject(columnName);
            field.set(entity, fieldValue);
        }
        return entity;
    }

    private String prepareSelectQuery(Class<?> type) {
        var tableAnnotation = type.getDeclaredAnnotation(Table.class);
        return String.format(SELECT_FROM_TABLE_BY_ID, tableAnnotation.value());
    }
}
