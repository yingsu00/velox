# Common connector plugin support library
add_library(velox_connector_common SHARED
        Connector.cpp
        ConnectorNames.h
        ConnectorObjectFactory.h
        ConnectorObjectFactory.cpp
        PluginLoader.cpp
)

target_include_directories(velox_connector_common
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>        # “include from source” when building
        $<INSTALL_INTERFACE:include/velox/connectors/common>  # “include from install” when consuming
)

# velox_connector_common itself needs velox_common and velox_dynamic_library_loader when it’s built, but anything that
# links against velox_connector_common does not automatically inherit those dependencies.
target_link_libraries(velox_connector_common
        PRIVATE
#        velox_common           # core Velox symbols
        velox_dynamic_library_loader
)

# Set the shared‐library version and SOVERSION as needed
set_target_properties(velox_connector_common PROPERTIES
        VERSION ${VELOX_VERSION}
        SOVERSION ${VELOX_SOVERSION}
)

# Install the shared library and its headers
install(TARGETS velox_connector_common
        ARCHIVE DESTINATION lib #Installs the static library (libvelox_connector_common.a) into the <install-prefix>/lib directory.
        LIBRARY DESTINATION lib #Installs any shared library (libvelox_connector_common.dylib or .so) into the same <install-prefix>/lib directory.
        INCLUDES DESTINATION include/velox/connectors/common
)

install(
        FILES
        Connector.h
        ConnectorNames.h
        PluginLoader.h
        ConnectorObjectFactory.h
        DESTINATION include/velox/connectors/common
)
