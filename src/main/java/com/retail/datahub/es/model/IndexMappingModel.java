package com.retail.datahub.es.model;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.es.model.IndexMappingModel <br/>
 * date: 2016-11-28 18:31 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class IndexMappingModel {

    private String field; //字段名称

    private String fieldType; //字段类型

    private String indexAnalyzed; //使用的分词器

    private Boolean store = true; //是否索引

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getIndexAnalyzed() {
        return indexAnalyzed;
    }

    public void setIndexAnalyzed(String indexAnalyzed) {
        this.indexAnalyzed = indexAnalyzed;
    }

    public Boolean getStore() {
        return store;
    }

    public void setStore(Boolean store) {
        this.store = store;
    }
}
