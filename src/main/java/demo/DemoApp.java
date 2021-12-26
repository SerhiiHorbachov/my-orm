package demo;

import com.orm.SessionFactory;
import demo.entity.Person;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;

public class DemoApp {

//    private static EntityManagerFactory emf;

    public static void main(String[] args) {
        var dataSource = initializeDataSource();
        var sessionFactory = new SessionFactory(dataSource);
        var session = sessionFactory.createSession();

        var person = session.find(Person.class, 1L);
        System.out.println(person);

        var theSamePerson = session.find(Person.class, 1L);
        System.out.println(theSamePerson);
        System.out.println(person == theSamePerson);
        person.setFirstName("Changed");


        session.close();

    }

    private static DataSource initializeDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL("jdbc:postgresql://localhost:5432/postgres");
        dataSource.setUser("postgres");
        dataSource.setPassword("password");

        return dataSource;
    }
}
