# The upstream pq-extra target uses native server/client development files.
# Its version probe must link the same installation, not Conan's client-only libpq.
if(NOT TARGET PostgreSQL::PostgreSQL)
  find_library(SERVICELIB_NATIVE_POSTGRESQL_CLIENT
    NAMES pq
    PATHS "${USERVER_PG_LIBRARY_DIR}"
    NO_DEFAULT_PATH REQUIRED)
  add_library(PostgreSQL::PostgreSQL UNKNOWN IMPORTED)
  set_target_properties(PostgreSQL::PostgreSQL PROPERTIES
    IMPORTED_LOCATION "${SERVICELIB_NATIVE_POSTGRESQL_CLIENT}"
    INTERFACE_INCLUDE_DIRECTORIES "${USERVER_PG_INCLUDE_DIR}")
endif()
set(PostgreSQL_FOUND TRUE)
