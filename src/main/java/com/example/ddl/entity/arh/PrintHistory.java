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
@Table(name = "T_PRINT_HISTORY", schema = "dbo", catalog = "arh", indexes = {
    @Index(name = "IX_USER_ID", columnList = "USER_ID"),
    @Index(name = "IX_PRINT_TIME", columnList = "PRINT_TIME")
})
@SequenceGenerator(
    name = "PrintHistory",
    sequenceName = "PRINT_HISTORY_SEQ",
    allocationSize = 1,
    schema = "dbo",
    catalog = "arh"
)
public class PrintHistory {

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "PrintHistory")
  @Column(name = "HISTORY_ID", nullable = false)
  private Long id;
  @Column(name = "USER_ID", nullable = false, length = 50)
  private String userId;
  @Column(name = "PRINT_TIME")
  private LocalDateTime printTime;
  @Column(name = "PAGE_COUNT")
  private Integer pageCount;
  @Column(name = "PRINTER_NAME", length = 100)
  private String printerName;
}
