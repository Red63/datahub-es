package com.retail.datahub.es.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.retail.datahub.es.exception.EsOperationException;
import com.retail.datahub.es.util.JSONUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;

import java.util.*;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.es.model.SqlResponse <br/>
 * date: 2016/6/6 14:30 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class SqlResponse {

    private SearchResponse searchResponse;

    private Set<Map<String, Object>> rows = new LinkedHashSet<>();

    private Map<String, Object> row = new HashMap<>();

    private boolean callChild = false;

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public SqlResponse() {
    }

    /**
     * 查询后的响应对象
     *
     * @param searchResponse
     */
    public SqlResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }


    /**
     * 是否有聚合查询结果
     *
     * @return
     */
    private boolean isAggregation() {
        if (searchResponse == null) return false;

        return searchResponse.getAggregations() != null;
    }

    /**
     * 是否有查询结果
     *
     * @return
     */
    private boolean isHits() {
        if (searchResponse == null) return false;

        return searchResponse.getHits() != null;
    }


    /**
     * map对象转业务实体对象
     *
     * @param map
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    /*private <T> T mapToObject(Map<String, Object> map, Class<T> clazz) throws Exception {
        if (map == null)
            return null;

        T obj = clazz.newInstance();

        org.apache.commons.beanutils.BeanUtils.populate(obj, map);

        return obj;
    }*/
    private <T> T mapToObject(Map<String, Object> map, Class<T> clazz) throws Exception {
        return JSONUtil.mapToBeanNotContainUnknown(map, clazz);
    }

    /**
     * 讲es的field域转换为你Object
     *
     * @param fields
     * @return
     */
    private Map<String, Object> toFieldsMap(Map<String, SearchHitField> fields) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, SearchHitField> entry : fields.entrySet()) {
            if (entry.getValue().values().size() > 1) {
                result.put(entry.getKey(), entry.getValue().values());
            } else {
                result.put(entry.getKey(), entry.getValue().value());
            }

        }
        return result;
    }

    /**
     * 获取hits结果
     *
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> List<T> hitsResult(Class<T> clazz) throws Exception {
        if (isHits()) {
            if (isAggregation()) {
                throw new EsOperationException("The method of access errors, provides you with aggregationResult() method...");
            }
        }

        List<T> results = new ArrayList<>();

        SearchHits hits = searchResponse.getHits();
        for (SearchHit searchHit : hits.getHits()) {
            if (searchHit.getSource() != null) {
                results.add(mapToObject(searchHit.getSource(), clazz));
            } else if (searchHit.getFields() != null) {
                Map<String, SearchHitField> fields = searchHit.getFields();
                results.add(mapToObject(toFieldsMap(fields), clazz));
            }

        }

        return results;
    }


    /**
     * 聚合查询返回结果
     * aggregationHandleResult object
     * Handle the query result,
     * in case of Aggregation query
     * (SQL group by)
     */
    public JSONArray aggregationHandleResult() throws Exception {
        if (isHits()) {
            if (!isAggregation()) {
                throw new EsOperationException("The method of access errors, provides you with hitsResult() method...");
            }
        }

        String responseJson = searchResponse.toString();

        JSONObject aggregations = JSON.parseObject(responseJson).getJSONObject("aggregations");
        if (aggregations.isEmpty()) return null;

        removeNestedAndFilters(aggregations);

        JSONArray rows = getRows("", aggregations, new JSONObject());

        return rows;
    }


    private void removeNestedAndFilters(JSONObject jsonObject) {
        Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();

        for (Map.Entry<String, Object> entry : entries) {
            if (StringUtils.endsWithAny(entry.getKey(), new String[]{"@NESTED", "@FILTER", "@NESTED_REVERSED", "@CHILDREN"})) {
                JSONObject subJsonObj = (JSONObject) entry.getValue();
                subJsonObj.remove("doc_count");
                subJsonObj.remove("key");

                Set<String> keySet = subJsonObj.keySet();
                for (String key : keySet) {
                    jsonObject.put(key, subJsonObj.get(key));
                }
                jsonObject.remove(entry.getKey());

                removeNestedAndFilters(jsonObject);
            }

            if (entry.getValue() instanceof JSONObject) {
                removeNestedAndFilters((JSONObject) entry.getValue());
            }
        }
    }

    private JSONArray getRows(String bucketName, JSONObject bucket, JSONObject additionalColumns) {
        JSONArray rows = new JSONArray();

        JSONArray subBuckets = getSubBuckets(bucket);
        if (subBuckets != null && subBuckets.size() > 0) {
            for (int i = 0; i < subBuckets.size(); i++) {
                String subBucketName = (String) ((JSONObject) subBuckets.get(i)).get("bucketName");
                JSONObject subBucket = (JSONObject) ((JSONObject) subBuckets.get(i)).get("bucket");

                JSONObject newAdditionalColumns = new JSONObject();
                // bucket without parents.
                if (StringUtils.isNotBlank(bucketName)) {
                    JSONObject newColumn = new JSONObject();
                    newColumn.put(bucketName, bucket.get("key"));

                    newAdditionalColumns = extend(newColumn, additionalColumns);
                }

                JSONArray newRows = getRows(subBucketName, subBucket, newAdditionalColumns);

                if(newRows.size() > 0) {
                    rows.addAll(newRows);
                }
            }
        } else {
            JSONObject obj = extend(new JSONObject(), additionalColumns);
            if (StringUtils.isNotBlank(bucketName)) {
                Set<String> keySet = bucket.keySet();
                if (keySet != null && keySet.contains("key_as_string")) {
                    obj.put(bucketName, bucket.get("key_as_string"));
                } else {
                    obj.put(bucketName, bucket.get("key"));
                }
            }

            Set<String> bucketKeySet = bucket.keySet();
            if (CollectionUtils.isNotEmpty(bucketKeySet)) {
                for (String bKey : bucketKeySet) {
                    Object bucketValue = bucket.get(bKey);
                    if (bucketValue instanceof JSONObject) {
                        JSONObject bucketValueJsonObj = (JSONObject) bucketValue;
                        Object tempBuckets = bucketValueJsonObj.get("buckets");
                        if (tempBuckets != null) {
                            if (tempBuckets instanceof JSONArray) { //size=0
                                JSONArray tempBucketsJsonArray = (JSONArray) tempBuckets;
                                if(tempBucketsJsonArray.size() > 0) {
                                    rows.addAll(tempBucketsJsonArray);
                                }

                                continue;
                            }

                            JSONObject buckets = (JSONObject) tempBuckets;
                            JSONArray newRows = getRows("", buckets, new JSONObject());
                            if (newRows.size() > 0){
                                rows.addAll(newRows);
                            }
                            continue;
                        }
                    }

                    if (bucketValue instanceof JSONObject) {
                        JSONObject bucketValueJsonObj = (JSONObject) bucketValue;
                        if (bucketValueJsonObj.get("value") != null) {
                            Set<String> keySet = bucketValueJsonObj.keySet();
                            if (CollectionUtils.isNotEmpty(keySet) && keySet.contains("value_as_string")) {
                                obj.put(bKey, bucketValueJsonObj.get("value_as_string"));
                            } else {
                                obj.put(bKey, bucketValueJsonObj.get("value"));
                            }
                        }
                    } else {
                        if (bucketValue instanceof JSONObject) {
                            JSONObject tempBucketValue = (JSONObject) bucketValue;
                            fillFieldsForSpecificAggregation(obj, tempBucketValue, bKey);
                        }
                    }
                }
            }

            if (obj.size() > 0) {
                rows.add(obj);
            }
        }

        return rows;
    }

    private JSONArray getSubBuckets(JSONObject bucket) {
        JSONArray subBuckets = new JSONArray();

        Set<String> keys = bucket.keySet();
        for (String key : keys) {
            if (bucket.get(key) instanceof JSONObject) {
                JSONArray buckets = (JSONArray) ((JSONObject) bucket.get(key)).get("buckets");
                if (buckets != null && buckets.size() > 0) {
                    for (int i = 0; i < buckets.size(); i++) {
                        JSONObject tmp = new JSONObject();
                        tmp.put("bucketName", key);
                        tmp.put("bucket", buckets.get(i));
                        subBuckets.add(tmp);
                    }
                } else {
                    JSONObject innerAgg = (JSONObject) bucket.get(key);
                    Set<String> innerKeys = innerAgg.keySet();
                    for (String innerKey : innerKeys) {
                        if (innerAgg.get(innerKey) instanceof JSONObject) {
                            JSONArray innerBuckets = getSubBuckets((JSONObject) innerAgg.get(innerKey));
                            subBuckets.addAll(innerBuckets);
                        }
                    }

                }
            }

        }


        return subBuckets;
    }

    private void fillFieldsForSpecificAggregation(JSONObject obj, JSONObject value, String field) {
        Set<String> keySet = value.keySet();
        if (CollectionUtils.isNotEmpty(keySet)) {
            for (String key : keySet) {
                if (StringUtils.equals(key, "values")) {
                    JSONObject tempValue = (JSONObject) value.get(key);
                    fillFieldsForSpecificAggregation(obj, tempValue, field);
                } else {
                    String tempKey = field + "." + key;
                    obj.put(tempKey, value.get(key));
                }
            }
        }
    }


    private JSONObject extend(JSONObject target, JSONObject source) {
        if (source != null && source.size() > 0) {
            Set<Map.Entry<String, Object>> entries = source.entrySet();
            for (Map.Entry entry : entries) {
                target.put((String) entry.getKey(), entry.getValue());
            }
        }

        return target;
    }

    /**
     * 聚合查询返回结果
     * deprecated
     *
     *@see com.retail.datahub.es.model.SqlResponse aggregationHandleResult() method
     *
     * @return
     * @throws Exception
     */
    @Deprecated
    public Set<Map<String, Object>> aggregationResult() throws Exception {
        if (isHits()) {
            if (!isAggregation()) {
                throw new EsOperationException("The method of access errors, provides you with hitsResult() method...");
            }
        }

        String response = searchResponse.toString();

        Map<String, Object> responseMap = JSONObject.parseObject(response);
        Map<String, Object> aggregationMap = (Map<String, Object>) responseMap.get("aggregations");

        Map<String, Object> result = new HashMap<>();
        String parentKey = "";
        Map<String, Object> parentValue = null;

        for (Map.Entry entry : aggregationMap.entrySet()) {
            parentKey = String.valueOf(entry.getKey());
            parentValue = (Map<String, Object>) entry.getValue();
        }
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) parentValue.get("buckets");

        if (buckets == null) {
            row.put(parentKey, parentValue.get("value"));
            rows.add(row);
        } else {
            getSubBuckets(buckets, parentKey);
        }

        return rows;
    }

    /**
     * 递归子节点
     *
     * @param buckets
     * @param prentKey
     * @return
     */
    private Set<Map<String, Object>> getSubBuckets(List<Map<String, Object>> buckets, String prentKey) {

        for (Map<String, Object> map : buckets) {

            if (!callChild) {
                row = new HashMap<>();
            }

            row.put(prentKey, map.get("key"));
            List<String> subkeys = subKey(map);

            for (String subKey : subkeys) {

                List<Map<String, Object>> subs = null;
                if (StringUtils.isNotEmpty(subKey)) {
                    subs = (List<Map<String, Object>>) ((Map<String, Object>) map.get(subKey)).get("buckets");
                }

                if (subs == null) {
                    Map<String, Object> endMap = (Map<String, Object>) map.get(subKey);
                    if (endMap != null) {
                        if (endMap.size() > 1) {
                            row.putAll(endMap);
                        } else {
                            row.put(subKey, endMap.get("value"));
                        }

                    }

                    callChild = false;
                } else {
                    callChild = true;
                    getSubBuckets(subs, subKey);
                }
            }

            rows.add(row);
        }
        return rows;
    }

    /**
     * 获取子节点的key名字
     *
     * @param bucket
     * @return
     */
    private List<String> subKey(Map<String, Object> bucket) {
        List<String> subkeys = new ArrayList<>();

        for (Map.Entry entry : bucket.entrySet()) {
            if (!StringUtils.equals("doc_count", String.valueOf(entry.getKey()))
                    && !StringUtils.equals("doc_count_error_upper_bound", String.valueOf(entry.getKey()))
                    && !StringUtils.equals("key", String.valueOf(entry.getKey()))) {

                subkeys.add(String.valueOf(entry.getKey()));
            }
        }

        return subkeys;
    }

}