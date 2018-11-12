package com.globalids.transform;

import com.globalids.transform.model.FilterModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by debasish paul on 30-10-2018.
 */
public class FilterBuilder {
    public static Dataset<Row> applyFilter(List<FilterModel> filterModels, Dataset<Row> dataset) {
        for (FilterModel filterModel : filterModels) {
            if(filterModel.getFilterValue().equalsIgnoreCase("equals")){
                dataset=dataset.filter(col(filterModel.getColumnName()).$eq$eq$eq(filterModel.getFilterValue()));
            }else if(filterModel.getFilterValue().equalsIgnoreCase("notEquals")){
                dataset=dataset.filter(col(filterModel.getColumnName()).notEqual(filterModel.getFilterValue()));
            }else if(filterModel.getFilterValue().equalsIgnoreCase("greater")){
                dataset=dataset.filter(col(filterModel.getColumnName()).$greater(filterModel.getFilterValue()));
            }else if(filterModel.getFilterValue().equalsIgnoreCase("greaterEquals")){
                dataset=dataset.filter(col(filterModel.getColumnName()).$greater$eq(filterModel.getFilterValue()));
            }else if(filterModel.getFilterValue().equalsIgnoreCase("less")){
                dataset=dataset.filter(col(filterModel.getColumnName()).$less(filterModel.getFilterValue()));
            }else if(filterModel.getFilterValue().equalsIgnoreCase("lessEquals")){
                dataset=dataset.filter(col(filterModel.getColumnName()).$less$eq(filterModel.getFilterValue()));
            }else if(filterModel.getFilterValue().equalsIgnoreCase("startWidth")){
                dataset=dataset.filter(col(filterModel.getColumnName()).startsWith(filterModel.getFilterValue()));
            }else if(filterModel.getFilterValue().equalsIgnoreCase("endWidth")){
                dataset=dataset.filter(col(filterModel.getColumnName()).endsWith(filterModel.getFilterValue()));
            }else if(filterModel.getFilterValue().equalsIgnoreCase("rlike")){
                dataset=dataset.filter(col(filterModel.getColumnName()).rlike(filterModel.getFilterValue()));
            }else if(filterModel.getFilterValue().equalsIgnoreCase("like")){
                dataset=dataset.filter(col(filterModel.getColumnName()).like(filterModel.getFilterValue()));
            }
        }
        return dataset;
    }
}
