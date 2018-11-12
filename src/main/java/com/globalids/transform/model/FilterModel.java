package com.globalids.transform.model;

/**
 * Created by debasish paul on 30-10-2018.
 */
public class FilterModel {
    String columnName;
    String filterValue;
    String operator;
    String condition;

    public FilterModel(String columnName, String filterValue, String operator, String condition) {
        this.columnName = columnName;
        this.filterValue = filterValue;
        this.operator = operator;
        this.condition = condition;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getFilterValue() {
        return filterValue;
    }

    public void setFilterValue(String filterValue) {
        this.filterValue = filterValue;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }
}
