//
// Created by Minh Khoa Tran on 13/9/25.
//
#include <string>
#include <fstream>

#include <arrow/table.h>
#include <arrow/status.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/writer.h"
#include "rapidjson/ostreamwrapper.h"


namespace FileIOUtil {
rapidjson::Document readJsonFile(const std::filesystem::path& path) {
  if (!std::filesystem::exists(path))
    throw std::runtime_error("schema file does not exist " + path.string());

  std::ifstream inf(path, std::ios::in | std::ios::binary);
  if (!inf.is_open())
    throw std::runtime_error("cannot open file " + path.string());

  std::string json((std::istreambuf_iterator<char>(inf)), {});
  if (json.empty())
    throw std::runtime_error("schema file is empty " + path.string());

  rapidjson::Document d;
  if (d.Parse(json.c_str()).HasParseError())
    throw std::runtime_error(std::string("parsing JSON err: ") + rapidjson::GetParseError_En(d.GetParseError()));

  return d;
}

void writeJsonFile(const std::filesystem::path& path, const rapidjson::Document& d) {
  std::ofstream ofs(path);
  rapidjson::OStreamWrapper osw(ofs);
  rapidjson::Writer<rapidjson::OStreamWrapper> writer(osw);
  d.Accept(writer);
}

arrow::Status writeParquetFile(const std::filesystem::path& path, const std::shared_ptr<arrow::Table>& table) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(path));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 5));

  return arrow::Status::OK();
}
}