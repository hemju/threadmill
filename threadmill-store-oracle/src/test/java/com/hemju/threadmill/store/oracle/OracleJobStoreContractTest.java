package com.hemju.threadmill.store.oracle;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.AbstractJobStoreContractTest;
import com.hemju.threadmill.test.ClaimPoisonRegression;

/**
 * Runs the {@link AbstractJobStoreContractTest} against a real Oracle database.
 * The exact suite the in-memory, PostgreSQL, and Redis stores pass must pass
 * here — with every borrowed connection starting at {@code autoCommit=false}
 * and asserting that the store restores that mode.
 */
class OracleJobStoreContractTest extends AbstractJobStoreContractTest {

  private DataSource dataSource;

  @BeforeEach
  void resetDatabase() throws Exception {
    dataSource = OracleTestDatabase.dataSource();
    OracleTestDatabase.reset();
  }

  @Test
  void poisonSerializationDoesNotDiscardEarlierClaims() {
    ClaimPoisonRegression.verify(
        new OracleJobStore(dataSource, ClaimPoisonRegression.serializer(), store.capabilities()));
  }

  @Override
  protected JobStore createStore() {
    return new OracleJobStore(new NonAutoCommitDataSource(OracleTestDatabase.dataSource()));
  }
}
