//
// Created by Minh Khoa Tran on 13/9/25.
//

#ifndef BTC_MODELS_H
#define BTC_MODELS_H

#include <vector>
#include <string>

struct PriceMetadata {
  int         timeTo;
  int         timeFrom;
  std::string response;
  std::string message;
  bool        hasWarning;
  int         pkgType;
  bool        aggregated;
};

struct PriceData {
  int    time;
  double open, high, low, close;
  double volumeFrom;
  double volumeTo;
};

struct ParseResult {
  PriceMetadata metadata;
  std::vector<PriceData> prices;
};

#endif //BTC_MODELS_H
