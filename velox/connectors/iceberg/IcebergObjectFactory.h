//
// Created by Ying Su on 6/20/25.
//

#pragma once

#include "velox/connectors/common/ConnectorObjectFactory.h"

class IcebergObjectFactory : public common::ConnectorObjectFactory {
  std::shared_ptr<ConnectorInsertTableHandle>
  IcebergObjectFactory::makeInsertTableHandle(
      const std::string& connectorId,
      std::vector<std::shared_ptr<const ConnectorColumnHandle>> inputColumns,
      std::shared_ptr<const ConnectorLocationHandle> locationHandle,
      const folly::dynamic& options) const
};
