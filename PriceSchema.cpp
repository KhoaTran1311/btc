//
// Created by Minh Khoa Tran on 13/9/25.
//

#include "PriceSchema.h"
#include "FileIOUtil.h"

#include <fstream>
#include <sstream>

PriceSchema::PriceSchema(const std::filesystem::path& path) {
  rapidjson::Document sd = FileIOUtil::readJsonFile(path);
  schema_ = std::make_unique<rapidjson::SchemaDocument>(sd);
}

void PriceSchema::validateJson(const rapidjson::Document& d) const {
  rapidjson::SchemaValidator validator(*schema_);
  if (!d.Accept(validator)) {
    std::ostringstream oss;
    rapidjson::StringBuffer sb;

    validator.GetInvalidSchemaPointer().StringifyUriFragment(sb);
    oss << "\n  Invalid schema: " << sb.GetString()
        << "\n  Invalid keyword: " << validator.GetInvalidSchemaKeyword();
    sb.Clear();
    validator.GetInvalidDocumentPointer().StringifyUriFragment(sb);
    oss << "\n  Invalid document: " <<  sb.GetString() << '\n';

    throw std::runtime_error(oss.str());
  }
}