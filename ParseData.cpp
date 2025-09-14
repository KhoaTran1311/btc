#include <exception>
#include <filesystem>
#include <iostream>

#include <arrow/api.h>
#include "rapidjson/document.h"

#include "FileIOUtil.h"
#include "models.h"
#include "PriceSchema.h"

namespace JsonKeys {
  constexpr char RESPONSE[]    = "Response";
  constexpr char MESSAGE[]     = "Message";
  constexpr char HAS_WARNING[] = "HasWarning";
  constexpr char TYPE[]        = "Type";
  constexpr char DATA[]        = "Data";

  namespace Data {
    constexpr char AGGREGATED[] = "Aggregated";
    constexpr char TIME_FROM[]  = "TimeFrom";
    constexpr char TIME_TO[]    = "TimeTo";
    constexpr char DATA[]       = "Data";

    namespace NestedData {
      constexpr char PRICE_TIME[] = "time";
      constexpr char PRICE_HIGH[] = "high";
      constexpr char PRICE_LOW[]  = "low";
      constexpr char PRICE_OPEN[] = "open";
      constexpr char VOLUME_FROM[] = "volumefrom";
      constexpr char VOLUME_TO[]   = "volumeto";
      constexpr char PRICE_CLOSE[] = "close";
    }
  }
}

void extractMetadataInPlace(rapidjson::Document& d) {
  rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

  auto dataItr = d.FindMember(JsonKeys::DATA);
  if (dataItr==d.MemberEnd() || !dataItr->value.IsObject())
    throw std::runtime_error("cannot find DATA");
  auto& data = dataItr->value;

  const auto aggItr = data.FindMember(JsonKeys::Data::AGGREGATED);
  const auto tfItr  = data.FindMember(JsonKeys::Data::TIME_FROM);
  const auto ttItr  = data.FindMember(JsonKeys::Data::TIME_TO);
  if (aggItr == data.MemberEnd() || !aggItr->value.IsBool() ||
          tfItr  == data.MemberEnd() || !tfItr->value.IsInt() ||
          ttItr  == data.MemberEnd() || !ttItr->value.IsInt())
    throw std::runtime_error("cannot find DATA member");

  const bool aggregated = aggItr->value.GetBool();
  const int timeFrom = tfItr->value.GetInt();
  const int timeTO = ttItr->value.GetInt();

  d.AddMember(JsonKeys::Data::AGGREGATED, aggregated, allocator);
  d.AddMember(JsonKeys::Data::TIME_FROM , timeFrom  , allocator);
  d.AddMember(JsonKeys::Data::TIME_TO   , timeTO    , allocator);

  data.SetNull();
  d.RemoveMember(JsonKeys::DATA);
}

std::shared_ptr<arrow::Table> valArrToArrow(const rapidjson::Value& arr) {
  arrow::Int64Builder timeBuilder;
  arrow::DoubleBuilder oBuilder, hBuilder, lBuilder, cBuilder, volFBuilder, volTBuilder;

  for (rapidjson::Value::ConstValueIterator itr = arr.Begin(); itr != arr.End(); ++itr) {
    auto price = itr->GetObject();
    timeBuilder.Append(price[JsonKeys::Data::NestedData::PRICE_TIME].GetInt());
    oBuilder.Append(price[JsonKeys::Data::NestedData::PRICE_OPEN].GetDouble());
    hBuilder.Append(price[JsonKeys::Data::NestedData::PRICE_HIGH].GetDouble());
    lBuilder.Append(price[JsonKeys::Data::NestedData::PRICE_LOW].GetDouble());
    cBuilder.Append(price[JsonKeys::Data::NestedData::PRICE_CLOSE].GetDouble());
    volFBuilder.Append(price[JsonKeys::Data::NestedData::VOLUME_FROM].GetDouble());
    volTBuilder.Append(price[JsonKeys::Data::NestedData::VOLUME_TO].GetDouble());
  }

  std::shared_ptr<arrow::Array> time, open, high, low, close, volumneFrom, volumneTo;
  timeBuilder.Finish(&time);
  oBuilder.Finish(&open);
  hBuilder.Finish(&high);
  lBuilder.Finish(&low);
  cBuilder.Finish(&close);
  volFBuilder.Finish(&volumneFrom);
  volTBuilder.Finish(&volumneTo);

  std::shared_ptr<arrow::Field> time_f, open_f, high_f, low_f, close_f, volumneFrom_f, volumneTo_f;
  std::shared_ptr<arrow::Schema> schema;

  time_f = arrow::field("time", arrow::int64());
  open_f = arrow::field("open", arrow::float64());
  high_f = arrow::field("high", arrow::float64());
  low_f = arrow::field("low", arrow::float64());
  close_f = arrow::field("close", arrow::float64());
  volumneFrom_f = arrow::field("volumneFrom", arrow::float64());
  volumneTo_f = arrow::field("volumneTo", arrow::float64());

  schema = arrow::schema({ time_f, open_f, high_f, low_f, close_f, volumneFrom_f, volumneTo_f });

  std::shared_ptr<arrow::Table> table;
  return arrow::Table::Make(schema, { time, open, high, low, close, volumneFrom, volumneTo });
}

std::shared_ptr<arrow::Table> extractPriceArr(rapidjson::Document& d) {
  auto dataItr = d.FindMember(JsonKeys::DATA);
  if (dataItr == d.MemberEnd() || !dataItr->value.IsObject())
    throw std::runtime_error("cannot find DATA");

  auto data = dataItr->value.GetObject();
  auto priceArrItr = data.FindMember(JsonKeys::Data::DATA);
  if (priceArrItr == data.MemberEnd() || !priceArrItr->value.IsArray())
    throw std::runtime_error("cannot find DATA prices");
  const rapidjson::Value& priceArr = priceArrItr->value;

  return valArrToArrow(priceArr);
}

void processSingleJson(const std::filesystem::path& path, const PriceSchema& schema) {
  rapidjson::Document d = FileIOUtil::readJsonFile(path);
  schema.validateJson(d);

  auto newPath = path;
  newPath += ".parquet";
  std::shared_ptr<arrow::Table> arrowTable = extractPriceArr(d);
  FileIOUtil::writeParquetFile(newPath, arrowTable);

  extractMetadataInPlace(d);
  newPath = path;
  newPath += ".metadata";
  FileIOUtil::writeJsonFile(newPath, d);
}

int main(int argc, char *argv[]) {
  try {
    if (argc < 3) {
      std::cerr << "Usage: " << argv[0] << " <schema.json> <input1.json> [input2.json...]\n";
      return 1;
    }
    PriceSchema schema(argv[1]);

    for (int i = 2; i < argc; ++i) {
      processSingleJson(argv[i], schema);
    }
  } catch (std::exception& e) {
    std::cerr << "error: " << e.what() << "\n";
    return 1;
  }
  return 0;
}