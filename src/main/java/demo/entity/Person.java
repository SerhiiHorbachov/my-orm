package demo.entity;

import com.orm.annotations.Column;
import com.orm.annotations.Id;
import com.orm.annotations.Table;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Table("users")
@ToString
@Getter
@Setter
public class Person {
    @Id
    @Column("id")
    private Long id;
    @Column("first_name")
    private String firstName;
    @Column("last_name")
    private String lastName;
}
