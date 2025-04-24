package com.example.ddl.entity.global;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.Nationalized;

@Getter
@Setter
@Entity
@Table(name = "T_USER", schema = "dbo", catalog = "global")
public class User {
  @Id
  @Column(name = "USER_ID", nullable = false, length = 50)
  private String userId;

  @Nationalized
  @Column(name = "USER_NAME", nullable = false, length = 50)
  private String userName;

  @Column(name = "AGE")
  private Integer age;
}