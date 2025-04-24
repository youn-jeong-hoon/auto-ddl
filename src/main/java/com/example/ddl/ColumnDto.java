package com.example.ddl;

import org.hibernate.mapping.Column;
import org.hibernate.mapping.Property;

public record ColumnDto(Property property, Column column) {


}
