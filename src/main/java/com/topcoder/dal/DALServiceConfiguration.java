package com.topcoder.dal;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.springframework.beans.factory.InjectionPoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.topcoder.dal.util.StreamJdbcTemplate;

@Configuration
@ComponentScan("com.topcoder")
public class DALServiceConfiguration {

    @Bean
    @Scope("prototype")
    public Logger produceLogger(InjectionPoint injectionPoint) {
        return org.slf4j.LoggerFactory.getLogger(injectionPoint.getMember().getDeclaringClass());
    }

    @Bean
    public StreamJdbcTemplate streamJdbcTemplate(DataSource dataSource) {
        return new StreamJdbcTemplate(dataSource);
    }
}
