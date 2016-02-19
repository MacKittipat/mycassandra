package com.mackittipat.mycassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class App {

    public static void main( String[] args ) {

        SpringApplication.run(App.class, args);

        Cluster cluster;
        Session session;

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("slothsandra");

        ResultSet resultSet = session.execute("SELECT column_name, type " +
                "FROM system.schema_columns " +
                "WHERE keyspace_name='slothsandra' " +
                "AND columnfamily_name='message_by_channel'");

        resultSet.all().forEach(r -> {
            System.out.println(r.getString("column_name") + " | " + r.getString("type"));
        });

        cluster.close();
    }
}
