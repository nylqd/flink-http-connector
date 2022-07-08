package com.getindata.connectors.http.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.getindata.connectors.http.internal.table.lookup.TableSourceHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TableSourceHelperTest {

  @Mock private DataType dataType;

  @Mock private LogicalType logicalType;

  @BeforeEach
  public void setUp() {
    when(dataType.getLogicalType()).thenReturn(logicalType);
  }

  @Test
  void testNotComposite() {
    when(logicalType.getTypeRoot()).thenReturn(LogicalTypeRoot.BIGINT);

    assertThat(TableSourceHelper.getFieldNames(dataType)).isEmpty();
  }
}
