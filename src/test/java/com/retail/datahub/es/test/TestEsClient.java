package com.retail.datahub.es.test;

import com.alibaba.fastjson.JSONArray;
import com.retail.datahub.es.model.SqlResponse;
import com.retail.datahub.es.sdk.EsClient;
import com.retail.datahub.es.util.JSONUtil;
import org.junit.Test;
import java.util.HashMap;
import java.util.List;

/**
 * 描述:<br/>测试类<br/>
 * ClassName: com.retail.datahub.es.test.TestEsClient <br/>
 * date: 2016-11-23 10:49 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class TestEsClient {

    private static EsClient client;
    private static final String eslist = "10.13.3.25:9300,10.13.3.26:9300";
    private static final String clusterName = "elasticsearch";

    public TestEsClient() {
        client = new EsClient(eslist, clusterName, true);
        client.init();
    }


    @Test
    public void aggTest() throws Exception {
        String sql = "SELECT charge_type as charge_type  ,contract_no as contract_no ,sum(charge_tax) as charge_tax,sum(charge_untax) as charge_untax \n" +
                "FROM contract_year_index_test/contract_year_ledger_test  \n" +
                "where contract_no='16100200007'\n" +
                "GROUP BY charge_type ,contract_no";

        SqlResponse select = client.asSql().select(sql);

        JSONArray jsonArray = select.aggregationHandleResult();
        /*for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject object = (JSONObject) jsonArray.get(i);
            System.out.println(object.toJSONString());
        }*/

        List<HashMap> hashMaps = JSONUtil.jsonArrayToList(jsonArray);
        System.out.println(hashMaps.size());
        for (HashMap map : hashMaps) {
            System.out.println(map);
        }

    }
}
