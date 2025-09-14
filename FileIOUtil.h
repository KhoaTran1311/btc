//
// Created by Minh Khoa Tran on 13/9/25.
//

#ifndef BTC_FILEIO_H
#define BTC_FILEIO_H

#include <filesystem>

#include <arrow/table.h>
#include <arrow/status.h>
#include "rapidjson/document.h"

namespace FileIOUtil {
  rapidjson::Document readJsonFile(const std::filesystem::path& path);
  void writeJsonFile(const std::filesystem::path& path, const rapidjson::Document& d);
  arrow::Status writeParquetFile(const std::filesystem::path& path, const std::shared_ptr<arrow::Table>& table);
}


#endif //BTC_FILEIO_H
