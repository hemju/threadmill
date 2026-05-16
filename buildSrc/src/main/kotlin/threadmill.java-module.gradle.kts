plugins {
    id("threadmill.java-base")
}

// Common test wiring is declared per-module in each subproject build script,
// using the `libs` version catalog. Keeping that out of the convention plugin
// avoids the buildSrc catalog-accessor issue and keeps dependencies explicit.
