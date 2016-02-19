package com.mackittipat.mycassandra.webapp.controller;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.List;

@Controller
public class MainController {

    private static final Logger log = LoggerFactory.getLogger(MainController.class);

    @RequestMapping(value = "/index")
    public String index(Model model, @RequestParam String keySpace, @RequestParam String columnFamily) {
        log.info("columnFamilyName = {}", columnFamily);

        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect(keySpace);

        // Find partition/cluster key

        ResultSet resultSet = session.execute("SELECT column_name, type " +
                "FROM system.schema_columns " +
                "WHERE keyspace_name='" + keySpace + "' " +
                "AND columnfamily_name='" + columnFamily + "'");
        resultSet.all().forEach(r -> {
            System.out.println(r.getString("column_name") + " | " + r.getString("type"));
        });


        List<List<Object>> resultRowList = new ArrayList<>();

        // Find all column
        List<Object> resultColNameList = new ArrayList<>();
        resultSet = session.execute("SELECT * FROM " + columnFamily + " LIMIT 1");
        resultSet.all().forEach(row -> {
            row.getColumnDefinitions().forEach(definition -> {
                resultColNameList.add(definition.getName());
            });
        });
        resultRowList.add(resultColNameList);

        resultSet = session.execute("SELECT * FROM " + columnFamily);
        resultSet.all().forEach(row -> {
            List<Object> resultColList = new ArrayList<>();
            resultColNameList.forEach(colName -> {
                resultColList.add(row.getObject(colName.toString()).toString());
            });
            resultRowList.add(resultColList);
        });

        model.addAttribute("resultRowList", resultRowList);

        return "index";
    }
}
