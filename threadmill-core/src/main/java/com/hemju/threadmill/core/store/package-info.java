/**
 * The persistence SPI ({@link com.hemju.threadmill.core.store.JobStore}) and
 * its capability descriptor.
 *
 * <p>The contract is expressed in operations and guarantees, not in any
 * particular storage language; an abstract contract test in
 * {@code threadmill-test-support} pins down the semantics, and every
 * concrete store passes the same suite.
 */
package com.hemju.threadmill.core.store;
