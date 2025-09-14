//
// Created by Minh Khoa Tran on 13/9/25.
//

#ifndef BTC_PRICESCHEMA_H
#define BTC_PRICESCHEMA_H

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <sstream>

#include "rapidjson/schema.h"
#include "rapidjson/stringbuffer.h"


class PriceSchema {
public:
  explicit PriceSchema(const std::filesystem::path& path);
  const rapidjson::SchemaDocument& doc() const noexcept { return *schema_; };
  void validateJson(const rapidjson::Document& d) const;
private:
  std::unique_ptr<rapidjson::SchemaDocument> schema_;
};


#endif //BTC_PRICESCHEMA_H
