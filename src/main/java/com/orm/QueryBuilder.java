package com.orm;

import com.orm.annotations.Column;
import com.orm.annotations.Table;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

public class QueryBuilder {
    private static final String SELECT_FROM_TABLE_BY_ID = "select * from %s where id = ?";
    private static final String UPDATE_TEMPLATE = "UPDATE %s SET %s WHERE id = ?";

    public static String prepareSelectQuery(Class<?> type) {
        var tableAnnotation = type.getDeclaredAnnotation(Table.class);
        return String.format(SELECT_FROM_TABLE_BY_ID, tableAnnotation.value());
    }

    @SneakyThrows
    public static String prepareUpdateQuery(Object entity) {
        Class<?> type = entity.getClass();

        Field[] declaredFields = entity.getClass().getDeclaredFields();
        StringBuilder queryBuilder = new StringBuilder();

        for (Field field : declaredFields) {
            if (!field.getName().equals("id")) {
                field.setAccessible(true);
                String fieldName = field.getAnnotation(Column.class).value().toLowerCase();
                String value = field.get(entity).toString();
                queryBuilder.append(String.format("%s = '%s', ", fieldName, value));
            }
        }
        String query = queryBuilder.substring(0, queryBuilder.length() - 2);

        return String.format(UPDATE_TEMPLATE, defineTableName(type), query);
    }

    private static String defineTableName(Class<?> type) {
        return type.isAnnotationPresent(Table.class) ?
            type.getAnnotation(Table.class).value() :
            type.getSimpleName().toLowerCase();
    }

}
