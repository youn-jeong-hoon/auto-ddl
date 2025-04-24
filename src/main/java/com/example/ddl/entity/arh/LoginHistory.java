package com.example.ddl.entity.arh;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "T_LOGIN_HISTORY", schema = "dbo", catalog = "arh", indexes = {
    @Index(name = "IX_LOGIN_HISTORY_USER_ID", columnList = "USER_ID"),
    @Index(name = "IX_LOGIN_HISTORY_LOGIN_TIME", columnList = "LOGIN_TIME")
})
@SequenceGenerator(
    name = "LoginHistory",
    sequenceName = "LOGIN_HISTORY_SEQ",
    allocationSize = 1,
    schema = "dbo",
    catalog = "arh"
)
public class LoginHistory {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "HISTORY_ID", nullable = false)
  private Long id;
  @Column(name = "USER_ID", nullable = false, length = 50)
  private String userId;
  @Column(name = "LOGIN_TIME")
  private LocalDateTime loginTime;
}
